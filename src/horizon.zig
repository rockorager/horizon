const std = @import("std");
const io = @import("io/io.zig");

const log = std.log.scoped(.horizon);

const http = std.http;
const linux = std.os.linux;
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const HeadParser = http.HeadParser;

const assert = std.debug.assert;

const strings = struct {
    const crlf = "\r\n";
    const content_length = "Content-Length";
    const content_type = "Content-Type";
};

pub fn threadRun(
    gpa: Allocator,
    ring: *io.Ring,
    main: io.Ring,
) !void {
    ring.* = try main.initChild(64);

    while (true) {
        try ring.submitAndWait();

        while (ring.nextCompletion()) |cqe| {
            try handleCqe(ring, gpa, cqe, null);
        }
    }
}

pub const Server = struct {
    ring: io.Ring,

    rings: []io.Ring,
    next_ring: usize = 0,

    fd: posix.fd_t,
    addr: net.Address,
    addr_len: u32,
    accept_c: Completion,

    msg_ring_c: Completion,

    pub const Options = struct {
        entries: u16 = 32,
        addr: ?net.Address = null,
    };

    pub fn init(self: *Server, gpa: Allocator, opts: Options) !void {
        // Set the no. open files to system max
        const limit = try posix.getrlimit(posix.rlimit_resource.NOFILE);
        try posix.setrlimit(posix.rlimit_resource.NOFILE, .{ .cur = limit.max, .max = limit.max });

        const addr = if (opts.addr) |a|
            a
        else
            net.Address.parseIp4("127.0.0.1", 8080) catch unreachable;

        const flags = linux.SOCK.STREAM | linux.SOCK.CLOEXEC;
        const fd = try posix.socket(addr.any.family, flags, 0);
        try posix.setsockopt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        self.* = .{
            .ring = try .init(opts.entries),
            .rings = try gpa.alloc(io.Ring, 12),
            .fd = fd,
            .addr = addr,
            .addr_len = addr.getOsSockLen(),
            .accept_c = .{ .parent = .server, .op = .accept },
            .msg_ring_c = .{ .parent = .server, .op = .msg_ring },
        };

        try posix.bind(fd, &self.addr.any, addr.getOsSockLen());
        try posix.listen(fd, 64);

        try self.ring.accept(self.fd, &self.accept_c);
        self.accept_c.active = true;
    }

    pub fn deinit(self: *Server) void {
        self.ring.deinit();
        // TODO: proper shutdown of threads
    }

    pub fn run(self: *Server, gpa: Allocator) !void {
        for (self.rings) |*ring| {
            _ = try std.Thread.spawn(.{}, threadRun, .{ gpa, ring, self.ring });
        }

        while (true) {
            try self.ring.submitAndWait();

            while (self.ring.nextCompletion()) |cqe| {
                try handleCqe(&self.ring, gpa, cqe, self);
            }
            // _ = try self.ring.submitAndWait();
            //
            // const n = self.ring.ring.copy_cqes(&cqes, 1) catch |err| {
            //     switch (err) {
            //         error.SignalInterrupt => {},
            //         else => log.err("copying cqes: {}", .{err}),
            //     }
            //     continue;
            // };
            // for (cqes[0..n]) |cqe| {
            //     try handleCqe(&self.ring, gpa, cqe, self);
            // }
        }
    }

    /// Accept a connection and send it to a thread to work on it
    fn acceptAndSend(
        self: *Server,
        ring: *io.Ring,
        cqe: io.Completion,
        target: io.Ring,
    ) CallbackError!void {
        const result = cqe.unwrap() catch |err| {
            switch (err) {
                error.Canceled => {},
                else => log.err("accept unwrap: {}", .{err}),
            }
            return;
        };

        if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
            // Multishot accept will return if it wasn't able to accept the next connection. Requeue it
            try ring.accept(self.fd, &self.accept_c);
            self.accept_c.active = true;
        }

        // Send the fd to another thread for handling
        try ring.msgRing(target, result, &self.accept_c, &self.msg_ring_c);
    }
};

pub fn handleCqe(
    ring: *io.Ring,
    gpa: Allocator,
    cqe: io.Completion,
    server: ?*Server,
) !void {
    const c: *Completion = @ptrFromInt(cqe.userdata);
    switch (c.parent) {
        .server => switch (c.op) {
            .accept => {
                const srv: *Server = @alignCast(@fieldParentPtr("accept_c", c));
                if (server) |s| {
                    const target = s.rings[s.next_ring];
                    s.next_ring = (s.next_ring + 1) % (s.rings.len);
                    try srv.acceptAndSend(&s.ring, cqe, target);
                } else {
                    try handleAccept(gpa, ring, cqe);
                }
            },
            .msg_ring => {
                _ = cqe.unwrap() catch |err| log.err("msg not sent to ring: {}", .{err});
            },
            else => unreachable,
        },
        .connection => {
            switch (c.op) {
                .recv => {
                    const conn: *Connection = @alignCast(@fieldParentPtr("recv_c", c));
                    defer conn.maybeDestroy(gpa);
                    try conn.handleRecv(ring, cqe);
                },

                .send => {
                    const conn: *Connection = @alignCast(@fieldParentPtr("send_c", c));
                    defer conn.maybeDestroy(gpa);
                    try conn.handleSend(ring, cqe);
                },

                .close => {
                    const conn: *Connection = @alignCast(@fieldParentPtr("close_c", c));
                    defer conn.maybeDestroy(gpa);
                    _ = cqe.unwrap() catch |err|
                        log.err("connection closed with error: {}", .{err});
                    conn.active -= 1;
                },

                .cancel => {
                    const conn: *Connection = @alignCast(@fieldParentPtr("cancel_c", c));
                    defer conn.maybeDestroy(gpa);
                    conn.active -= 1;
                },

                else => unreachable,
            }
        },
    }
}

/// A Completion Entry
const Completion = struct {
    parent: enum {
        server,
        connection,
    },

    op: enum {
        accept,
        msg_ring,
        recv,
        send,
        close,
        cancel,
    },

    /// If the completion has been scheduled
    active: bool = false,
};

const CallbackError = Allocator.Error || error{
    /// Returned when a recv didn't have a buffer selected
    NoBufferSelected,
    SubmissionQueueFull,
};

fn handleAccept(
    gpa: Allocator,
    ring: *io.Ring,
    cqe: io.Completion,
) CallbackError!void {
    const result = cqe.unwrap() catch {
        log.err("accept error: {}", .{cqe.err()});
        @panic("accept error");
    };

    const conn = try gpa.create(Connection);
    try conn.init(
        gpa,
        ring,
        result,
    );
}

const Connection = struct {
    arena: std.heap.ArenaAllocator,

    buf: [1024]u8 = undefined,

    fd: posix.socket_t,

    recv_c: Completion = .{ .parent = .connection, .op = .recv },
    send_c: Completion = .{ .parent = .connection, .op = .send },
    close_c: Completion = .{ .parent = .connection, .op = .close },
    cancel_c: Completion = .{ .parent = .connection, .op = .cancel },

    /// Write buffer
    write_buf: std.ArrayListUnmanaged(u8) = .empty,

    /// Number of active completions.
    active: u8 = 0,

    /// An active request
    request: Request = .{},

    response: Response,

    vecs: [2]posix.iovec_const,
    written: usize = 0,

    pub fn init(
        self: *Connection,
        gpa: Allocator,
        ring: *io.Ring,
        fd: posix.socket_t,
    ) CallbackError!void {
        self.* = .{
            .arena = .init(gpa),
            .fd = fd,
            .response = undefined,
            .vecs = undefined,
        };

        self.response = .{ .arena = self.arena.allocator() };

        try ring.recv(self.fd, &self.buf, &self.recv_c);

        self.active += 1;
        self.recv_c.active = true;
    }

    pub fn maybeDestroy(self: *Connection, gpa: Allocator) void {
        if (self.active > 0) return;
        self.deinit();
        gpa.destroy(self);
    }

    pub fn deinit(self: *Connection) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn close(self: *Connection, ring: *io.Ring) !void {
        if (self.recv_c.active) {
            // Cancel recv_c if we have one out there
            try ring.cancel(&self.recv_c, &self.cancel_c);
            self.active += 1;
            self.cancel_c.active = true;
        }
        // Close downstream
        try ring.close(self.fd, &self.close_c);
        self.active += 1;
        self.close_c.active = true;
    }

    pub fn handleRecv(
        self: *Connection,
        ring: *io.Ring,
        cqe: io.Completion,
    ) !void {
        self.active -= 1;
        self.recv_c.active = false;
        if (cqe.result == 0) {
            // Peer gracefully closed connection
            return self.close(ring);
        }

        const result = cqe.unwrap() catch |err| {
            switch (err) {
                error.Canceled => return,
                error.ConnectionResetByPeer => return self.close(ring),
                else => {
                    log.err("recv error: {}", .{cqe.err()});
                    return self.close(ring);
                },
            }
        };

        try self.request.appendSlice(self.arena.allocator(), self.buf[0..result]);

        if (self.request.head_len == null) {
            // We haven't received a full HEAD. prep another recv
            try ring.recv(self.fd, &self.buf, &self.recv_c);
            self.active += 1;
            self.recv_c.active = true;
            return;
        }

        // We've received a full HEAD, call the handler now
        try handler(self.request, &self.response);
        try self.prepareHeader();
        try self.send(ring);
    }

    fn reset(self: *Connection) void {
        _ = self.arena.reset(.retain_capacity);
        self.request = .{};
        self.write_buf = .empty;
        self.response = .{ .arena = self.arena.allocator() };
        self.written = 0;
    }

    /// Writes the header into write_buf
    fn prepareHeader(self: *Connection) !void {
        var headers = &self.write_buf;
        const resp = self.response;
        const status = resp.status orelse .ok;

        // Base amount to cover start line, content-length, content-type, and trailing \r\n
        var len: usize = 128;
        {
            var iter = resp.headers.iterator();
            while (iter.next()) |entry| {
                len += entry.key_ptr.len + entry.value_ptr.len + 2;
            }
        }

        try headers.ensureTotalCapacity(self.arena.allocator(), len);
        var writer = headers.fixedWriter();

        // We never write the phrase
        writer.print("HTTP/1.1 {d}\r\n", .{@intFromEnum(status)}) catch unreachable;

        if (resp.headers.get(strings.content_length) == null) {
            writer.print(strings.content_length ++ ": {d}" ++ strings.crlf, .{resp.body.items.len}) catch unreachable;
        }

        if (resp.headers.get(strings.content_type) == null) {
            // TODO: sniff content type
            writer.print(strings.content_type ++ ": {s}" ++ strings.crlf, .{"text/plain"}) catch unreachable;
        }

        var iter = resp.headers.iterator();
        while (iter.next()) |h| {
            writer.print("{s}: {s}" ++ strings.crlf, .{ h.key_ptr.*, h.value_ptr.* }) catch unreachable;
        }

        writer.writeAll(strings.crlf) catch unreachable;
    }

    fn responseComplete(self: *Connection) bool {
        return self.written == (self.write_buf.items.len + self.response.body.items.len);
    }

    fn send(self: *Connection, ring: *io.Ring) !void {
        // Pending headers, no body
        if (self.written < self.write_buf.items.len and self.response.body.items.len == 0) {
            try ring.write(self.fd, self.write_buf.items[self.written..], &self.send_c);
            self.active += 1;
            self.send_c.active = true;
            return;
        }

        // Pending headers + body
        if (self.written < self.write_buf.items.len) {
            const headers = self.write_buf.items[self.written..];
            self.vecs[0] = .{
                .base = headers.ptr,
                .len = headers.len,
            };
            self.vecs[1] = .{
                .base = self.response.body.items.ptr,
                .len = self.response.body.items.len,
            };

            try ring.writev(self.fd, &self.vecs, &self.send_c);
            self.active += 1;
            self.send_c.active = true;

            return;
        }

        // Pending body
        const offset = self.written - self.write_buf.items.len;
        const body = self.response.body.items[offset..];
        self.active += 1;
        try ring.write(self.fd, body, &self.send_c);
        self.send_c.active = true;
    }

    fn handleSend(self: *Connection, ring: *io.Ring, cqe: io.Completion) CallbackError!void {
        self.active -= 1;
        self.send_c.active = false;
        switch (cqe.err()) {
            .SUCCESS => {},
            else => {
                log.err("send error: {}", .{cqe.err()});
                return self.close(ring);
            },
        }

        self.written += @intCast(cqe.result);

        switch (self.responseComplete()) {
            // If the response didn't finish sending, try again
            false => return self.send(ring),

            // Response sent. Decide if we should close the connection or keep it alive
            true => switch (self.request.keepAlive()) {
                false => return self.close(ring),

                true => {
                    self.reset();
                    // prep another recv
                    try ring.recv(self.fd, &self.buf, &self.recv_c);
                    self.active += 1;
                },
            },
        }
    }
};

pub const Request = struct {
    /// The raw bytes of the request. This may not be the full request - handlers are called when we
    /// have received the headers. At that point, callers can instruct the response to read the full
    /// body. In this case, the callback will be called again when the full body has been read
    bytes: std.ArrayListUnmanaged(u8) = .empty,

    /// Length of the head section. When null, we have not received enough bytes to finish the head.
    /// This includes the trailing empty line terminators
    head_len: ?usize = null,

    /// Body of the request. This is null if we haven't received the full body, or there is no body
    body: ?[]const u8 = null,

    // add the slice to the internal buffer, attempt to parse a Head
    pub fn appendSlice(self: *Request, gpa: Allocator, bytes: []const u8) !void {
        try self.bytes.appendSlice(gpa, bytes);

        const idx = std.mem.indexOf(u8, self.bytes.items, strings.crlf ++ strings.crlf) orelse return;
        assert(idx + 4 == self.bytes.items.len);
        self.head_len = idx + 4;

        const cl = self.contentLength() orelse return;

        if (cl + idx + 4 == self.bytes.items.len) {
            self.body = self.bytes.items[idx + 4 ..];
        }
    }

    pub fn headerIterator(self: Request) http.HeaderIterator {
        assert(self.head_len != null);
        return .init(self.bytes.items[0..self.head_len.?]);
    }

    pub fn getHeader(self: Request, key: []const u8) ?[]const u8 {
        var iter = self.headerIterator();
        while (iter.next()) |header| {
            // Check lengths
            if (header.name.len != key.len) continue;

            // Fast path for canonical case matched headers. We use those exclusively in this
            // library when getting headers
            if (std.mem.eql(u8, header.name, key) or
                // Slow path for case matching
                std.ascii.eqlIgnoreCase(header.name, key))
                return header.value;
        }
        return null;
    }

    /// Returns the HTTP method of this request
    pub fn method(self: Request) http.Method {
        assert(self.head_len != null);
        const idx = std.mem.indexOf(u8, self.bytes.items, strings.crlf) orelse unreachable;
        const line = self.bytes.items[0..idx];
        const space = std.mem.indexOfScalar(u8, line, ' ') orelse @panic("TODO: bad request");
        const val = http.Method.parse(line[0..space]);
        return @enumFromInt(val);
    }

    pub fn contentLength(self: Request) ?u64 {
        const value = self.getHeader(strings.content_length) orelse return null;
        return std.fmt.parseUnsigned(u64, value, 10) catch @panic("TODO: bad content length");
    }

    pub fn host(self: Request) []const u8 {
        return self.getHeader("Host") orelse @panic("TODO: no host header");
    }

    pub fn keepAlive(self: Request) bool {
        const value = self.getHeader("Connection") orelse return true;
        // fast and slow paths for case matching
        return std.mem.eql(u8, value, "keep-alive") or
            std.ascii.eqlIgnoreCase(value, "keep-alive");
    }
};

pub const Response = struct {
    arena: Allocator,

    body: std.ArrayListUnmanaged(u8) = .empty,

    headers: std.StringHashMapUnmanaged([]const u8) = .{},

    status: ?http.Status = null,

    pub fn setHeader(self: *Response, k: []const u8, v: []const u8) Allocator.Error!void {
        const k_dupe = try self.arena.dupe(u8, k);
        const v_dupe = try self.arena.dupe(u8, v);
        try self.headers.put(self.arena, k_dupe, v_dupe);
    }

    pub fn setStatus(self: *Response, status: http.Status) void {
        self.status = status;
    }

    pub fn write(self: *Response, bytes: []const u8) Allocator.Error!usize {
        try self.body.appendSlice(self.arena, bytes);
        return bytes.len;
    }

    pub fn writeAll(self: *Response, bytes: []const u8) Allocator.Error!void {
        try self.body.appendSlice(self.arena, bytes);
    }

    pub fn print(self: *Response, comptime fmt: []const u8, args: anytype) Allocator.Error!void {
        try self.body.writer(self.arena).print(fmt, args);
    }

    fn typeErasedWrite(ptr: *const anyopaque, bytes: []const u8) anyerror!usize {
        const self: *Response = @constCast(@ptrCast(@alignCast(ptr)));
        return self.write(bytes);
    }

    pub fn any(self: *Response) std.io.AnyWriter {
        return .{
            .context = self,
            .writeFn = typeErasedWrite,
        };
    }
};

fn handler(req: Request, resp: *Response) !void {
    _ = req;
    try resp.writeAll("hey");
}
