const std = @import("std");
const io = @import("io/io.zig");

const log = std.log.scoped(.horizon);

const http = std.http;
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
    handler: Handler,
) !void {
    ring.* = try main.initChild();

    while (true) {
        try ring.submitAndWait();

        while (ring.nextCompletion()) |cqe| {
            try handleCqe(ring, gpa, cqe, null, handler);
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
        /// Number of entries in the completion queues
        entries: u16 = 64,

        /// Address to listen on
        addr: ?net.Address = null,

        /// Number of worker threads to spawn. If null, we will spawn at least 1 worker, and up to
        /// threadcount - 1 workers. We require one worker because one thread is exclusively used
        /// for the accept loop. Another is used to handle requests. We always need at least 2, but
        /// no more than the core count
        workers: ?u16 = null,
    };

    pub fn init(self: *Server, gpa: Allocator, opts: Options) !void {
        // Set the no. open files to system max
        const limit = try posix.getrlimit(posix.rlimit_resource.NOFILE);
        try posix.setrlimit(posix.rlimit_resource.NOFILE, .{ .cur = limit.max, .max = limit.max });

        const addr = if (opts.addr) |a|
            a
        else
            net.Address.parseIp4("127.0.0.1", 8080) catch unreachable;

        const flags = posix.SOCK.STREAM | posix.SOCK.CLOEXEC;
        const fd = try posix.socket(addr.any.family, flags, 0);
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        const worker_count: usize = blk: {
            const cpu_count = std.Thread.getCpuCount() catch 1;
            const base = opts.workers orelse @max(1, cpu_count -| 1);
            break :blk @max(base, 1);
        };

        self.* = .{
            .ring = try .init(opts.entries),
            .rings = try gpa.alloc(io.Ring, worker_count),
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

    pub fn run(self: *Server, gpa: Allocator, handler: Handler) !void {
        for (self.rings) |*ring| {
            _ = try std.Thread.spawn(.{}, threadRun, .{ gpa, ring, self.ring, handler });
        }

        while (true) {
            try self.ring.submitAndWait();

            while (self.ring.nextCompletion()) |cqe| {
                try handleCqe(&self.ring, gpa, cqe, self, handler);
            }
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

        if (!cqe.flags.has_more) {
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
    handler: Handler,
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
                    try conn.handleRecv(ring, cqe, handler);
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
    try conn.init(gpa, ring, result);
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
        handler: Handler,
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

        if (!self.request.receivedHeader()) {
            // We haven't received a full HEAD. prep another recv
            try ring.recv(self.fd, &self.buf, &self.recv_c);
            self.active += 1;
            self.recv_c.active = true;
            return;
        }

        // We've received a full HEAD, call the handler now
        try handler.serveHttp(self.response.responseWriter(), self.request);
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
            writer.print(
                strings.content_length ++ ": {d}" ++ strings.crlf,
                .{resp.body.items.len},
            ) catch unreachable;
        }

        if (resp.headers.get(strings.content_type) == null) {
            // TODO: sniff content type
            writer.print(
                strings.content_type ++ ": {s}" ++ strings.crlf,
                .{"text/plain"},
            ) catch unreachable;
        }

        var iter = resp.headers.iterator();
        while (iter.next()) |h| {
            writer.print(
                "{s}: {s}" ++ strings.crlf,
                .{ h.key_ptr.*, h.value_ptr.* },
            ) catch unreachable;
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

    // add the slice to the internal buffer, attempt to parse a Head
    pub fn appendSlice(self: *Request, gpa: Allocator, bytes: []const u8) !void {
        try self.bytes.appendSlice(gpa, bytes);
    }

    pub fn headLen(self: Request) ?usize {
        const idx = std.mem.indexOf(
            u8,
            self.bytes.items,
            strings.crlf ++ strings.crlf,
        ) orelse return null;

        return idx + 4;
    }

    /// Returns true if we have received the full header
    pub fn receivedHeader(self: Request) bool {
        return self.headLen() != null;
    }

    /// Returns the body of the request. Null indicates there is a body and we haven't read the
    /// entirety of it. An empty string indicates the request has no body and is not expecting one
    pub fn body(self: Request) ?[]const u8 {
        const head_len = self.headLen() orelse return null;

        const cl = self.contentLength() orelse {
            // TODO: we need to also check for chunked transfer encoding
            return "";
        };

        if (cl + head_len == self.bytes.items.len) return self.bytes.items[head_len..];

        return null;
    }

    /// iterates over headers and trailers
    pub fn headerIterator(self: Request) HeaderIterator {
        assert(self.receivedHeader());
        return .init(self.bytes.items);
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
        assert(self.receivedHeader());
        // GET / HTTP/1.1
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

    pub fn path(self: Request) []const u8 {
        assert(self.receivedHeader());
        const idx = std.mem.indexOf(u8, self.bytes.items, strings.crlf) orelse unreachable;
        const line = self.bytes.items[0..idx];
        var iter = std.mem.splitScalar(u8, line, ' ');
        _ = iter.first();
        return iter.next() orelse unreachable;
    }
};

pub const ResponseWriter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        setHeader: *const fn (*anyopaque, []const u8, ?[]const u8) Allocator.Error!void,
        setStatus: *const fn (*anyopaque, status: http.Status) void,
        write: *const fn (*const anyopaque, []const u8) anyerror!usize,

        pub fn init(comptime T: type) *const VTable {
            return &.{
                .setHeader = T.setHeader,
                .setStatus = T.setStatus,
                .write = T.write,
            };
        }
    };

    pub fn init(comptime T: type, ptr: *T) ResponseWriter {
        return .{
            .ptr = ptr,
            .vtable = .init(T),
        };
    }

    pub fn setHeader(self: ResponseWriter, name: []const u8, value: ?[]const u8) Allocator.Error!void {
        return self.vtable.setHeader(self.ptr, name, value);
    }

    pub fn setStatus(self: ResponseWriter, status: http.Status) void {
        return self.vtable.setStatus(self.ptr, status);
    }

    pub fn write(self: ResponseWriter, bytes: []const u8) Allocator.Error!usize {
        return self.vtable.write(self.ptr, bytes);
    }

    pub fn any(self: ResponseWriter) std.io.AnyWriter {
        return .{ .context = self.ptr, .writeFn = self.vtable.write };
    }
};

pub const Response = struct {
    arena: Allocator,

    body: std.ArrayListUnmanaged(u8) = .empty,

    headers: std.StringHashMapUnmanaged([]const u8) = .{},

    status: ?http.Status = null,

    const VTable: ResponseWriter.VTable = .{
        .setHeader = Response.setHeader,
        .setStatus = Response.setStatus,
        .write = Response.typeErasedWrite,
    };

    fn responseWriter(self: *Response) ResponseWriter {
        return .{ .ptr = self, .vtable = &VTable };
    }

    fn setHeader(ptr: *anyopaque, k: []const u8, maybe_v: ?[]const u8) Allocator.Error!void {
        const self: *Response = @ptrCast(@alignCast(ptr));
        if (maybe_v) |v| {
            const k_dupe = try self.arena.dupe(u8, k);
            const v_dupe = try self.arena.dupe(u8, v);
            return self.headers.put(self.arena, k_dupe, v_dupe);
        }

        _ = self.headers.remove(k);
    }

    fn setStatus(ptr: *anyopaque, status: http.Status) void {
        const self: *Response = @ptrCast(@alignCast(ptr));
        self.status = status;
    }

    fn typeErasedWrite(ptr: *const anyopaque, bytes: []const u8) anyerror!usize {
        const self: *Response = @constCast(@ptrCast(@alignCast(ptr)));
        try self.body.appendSlice(self.arena, bytes);
        return bytes.len;
    }
};

pub const HeaderIterator = struct {
    bytes: []const u8,
    index: usize,
    is_trailer: bool,

    pub fn init(bytes: []const u8) HeaderIterator {
        return .{
            .bytes = bytes,
            .index = std.mem.indexOfPos(u8, bytes, 0, "\r\n").? + 2,
            .is_trailer = false,
        };
    }

    pub fn next(it: *HeaderIterator) ?std.http.Header {
        const end = std.mem.indexOfPosLinear(u8, it.bytes, it.index, "\r\n").?;
        if (it.index == end) { // found the trailer boundary (\r\n\r\n)
            if (it.is_trailer) return null;

            const next_end = std.mem.indexOfPos(u8, it.bytes, end + 2, "\r\n") orelse
                return null;

            var kv_it = std.mem.splitScalar(u8, it.bytes[end + 2 .. next_end], ':');
            const name = kv_it.first();
            const value = kv_it.rest();

            it.is_trailer = true;
            it.index = next_end + 2;
            if (name.len == 0)
                return null;

            return .{
                .name = name,
                .value = std.mem.trim(u8, value, " \t"),
            };
        } else { // normal header
            var kv_it = std.mem.splitScalar(u8, it.bytes[it.index..end], ':');
            const name = kv_it.first();
            const value = kv_it.rest();

            it.index = end + 2;
            if (name.len == 0)
                return null;

            return .{
                .name = name,
                .value = std.mem.trim(u8, value, " \t"),
            };
        }
    }
};

pub const Handler = struct {
    ptr: *anyopaque,
    serveFn: *const fn (*anyopaque, ResponseWriter, Request) anyerror!void,

    /// Initialize a handler from a Type which has a serveHttp method
    pub fn init(comptime T: type, ptr: *T) Handler {
        return .{ .ptr = ptr, .serveFn = T.serveHttp };
    }

    pub fn serveHttp(self: Handler, w: ResponseWriter, r: Request) anyerror!void {
        return self.serveFn(self.ptr, w, r);
    }
};
