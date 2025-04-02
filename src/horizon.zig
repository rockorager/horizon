const std = @import("std");
const tls = @import("tls");

const log = std.log.scoped(.horizon);

const http = std.http;
const linux = std.os.linux;
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const HeadParser = http.HeadParser;
const IoUring = linux.IoUring;

const assert = std.debug.assert;

pub fn threadRun(
    gpa: Allocator,
    ring: *IoUring,
    wq_fd: posix.fd_t,
) !void {
    {
        // Initialize the ring
        const flags2: u32 =
            linux.IORING_SETUP_SUBMIT_ALL | // Keep submitting events even if one had an error
            linux.IORING_SETUP_CLAMP | // Clamp entries to system supported max
            linux.IORING_SETUP_ATTACH_WQ | // Share work queue
            linux.IORING_SETUP_DEFER_TASKRUN | // Defer work until we submit tasks
            linux.IORING_SETUP_COOP_TASKRUN | // Don't interupt userspace when task is complete
            linux.IORING_SETUP_SINGLE_ISSUER; // Only a single thread will issue tasks

        var params2 = std.mem.zeroInit(linux.io_uring_params, .{
            .flags = flags2,
            .sq_thread_idle = 1000,
            .wq_fd = @as(u32, @intCast(wq_fd)),
        });

        ring.* = try .init_params(64, &params2);
    }

    var cqes: [128]linux.io_uring_cqe = undefined;
    while (true) {
        _ = try ring.submit();

        const n = ring.copy_cqes(&cqes, 1) catch |err| {
            switch (err) {
                error.SignalInterrupt => {},
                else => log.err("copying cqes: {}", .{err}),
            }
            continue;
        };
        for (cqes[0..n]) |cqe| {
            try handleCqe(ring, gpa, cqe, null);
        }
    }
}
pub const Server = struct {
    ring: IoUring,

    rings: []IoUring,
    next_ring: usize = 0,

    fd: posix.fd_t,
    addr: net.Address,
    addr_len: u32,
    accept_c: Completion,

    msg_ring_c: Completion,

    pub const Options = struct {
        entries: u16 = 256,
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

        const io_uring_flags: u32 =
            linux.IORING_SETUP_SUBMIT_ALL | // Keep submitting events even if one had an error
            linux.IORING_SETUP_CLAMP | // Clamp entries to system supported max
            linux.IORING_SETUP_DEFER_TASKRUN | // Defer work until we submit tasks
            linux.IORING_SETUP_COOP_TASKRUN | // Don't interupt userspace when task is complete
            linux.IORING_SETUP_SINGLE_ISSUER; // Only a single thread will issue tasks

        var params = std.mem.zeroInit(linux.io_uring_params, .{
            .flags = io_uring_flags,
            .sq_thread_idle = 1000,
        });

        const ring: linux.IoUring = try .init_params(opts.entries, &params);

        const flags = linux.SOCK.STREAM | linux.SOCK.CLOEXEC;
        const fd = try posix.socket(addr.any.family, flags, 0);
        try posix.setsockopt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        self.* = .{
            .ring = ring,
            .rings = try gpa.alloc(IoUring, 12),
            .fd = fd,
            .addr = addr,
            .addr_len = addr.getOsSockLen(),
            .accept_c = .{ .parent = .server, .op = .accept },
            .msg_ring_c = .{ .parent = .server, .op = .msg_ring },
        };

        try posix.bind(fd, &self.addr.any, addr.getOsSockLen());
        try posix.listen(fd, 512);

        const sqe = try getSqeWithRetry(&self.ring, 16);
        const accept_flags: u32 = 0;

        sqe.prep_multishot_accept(self.fd, &self.addr.any, &self.addr_len, accept_flags);
        sqe.user_data = @intFromPtr(&self.accept_c);
        self.accept_c.active = true;

        log.info("listening on {}", .{self.addr});
    }

    pub fn deinit(self: *Server) void {
        self.ring.deinit();
    }

    pub fn run(self: *Server, gpa: Allocator) !void {
        for (self.rings) |*ring| {
            _ = try std.Thread.spawn(.{}, threadRun, .{ gpa, ring, self.ring.fd });
        }

        var cqes: [128]linux.io_uring_cqe = undefined;
        while (true) {
            _ = try self.ring.submit_and_wait(1);

            const n = self.ring.copy_cqes(&cqes, 1) catch |err| {
                switch (err) {
                    error.SignalInterrupt => {},
                    else => log.err("copying cqes: {}", .{err}),
                }
                continue;
            };
            for (cqes[0..n]) |cqe| {
                try handleCqe(&self.ring, gpa, cqe, self);
            }
        }
    }

    /// Accept a connection and send it to a thread to work on it
    fn acceptAndSend(
        self: *Server,
        ring: *IoUring,
        cqe: linux.io_uring_cqe,
        target_fd: posix.fd_t,
    ) CallbackError!void {
        switch (cqe.err()) {
            .SUCCESS => {},
            .INVAL => @panic("invalid submission"),
            else => unreachable,
        }

        // Check if this is the last accept.
        if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
            // TODO: requeue the accept?
            log.err("accept completion complete", .{});
            log.err("cqe error: {}", .{cqe.err()});
        }

        log.debug("sending fd={d} to ring_fd={d}", .{ cqe.res, target_fd });

        // Send the fd to another thread for handling
        const sqe = try getSqeWithRetry(ring, 16);
        sqe.prep_rw(.MSG_RING, target_fd, 0, @intCast(cqe.res), @intFromPtr(&self.accept_c));
        sqe.user_data = @intFromPtr(&self.msg_ring_c);
        sqe.flags |= linux.IOSQE_CQE_SKIP_SUCCESS;
        return;
    }
};

pub fn handleCqe(
    ring: *IoUring,
    gpa: Allocator,
    cqe: linux.io_uring_cqe,
    server: ?*Server,
) !void {
    const c: *Completion = @ptrFromInt(cqe.user_data);
    switch (c.parent) {
        .server => switch (c.op) {
            .accept => {
                const srv: *Server = @alignCast(@fieldParentPtr("accept_c", c));
                if (server) |s| {
                    const target = s.rings[s.next_ring].fd;
                    s.next_ring += 1;
                    if (s.next_ring == s.rings.len) s.next_ring = 0;
                    try srv.acceptAndSend(ring, cqe, target);
                    // try handleAccept(gpa, ring, cqe, bg.group_id);
                } else {
                    try handleAccept(gpa, ring, cqe);
                }
            },
            .msg_ring => {
                switch (cqe.err()) {
                    .SUCCESS => {},
                    else => log.err("msg not sent to ring: {}", .{cqe.err()}),
                }
            },
            else => unreachable,
        },
        .connection => {
            switch (c.op) {
                .recv => {
                    const conn: *Connection = @alignCast(@fieldParentPtr("recv_c", c));
                    defer conn.maybeDestroy(gpa);
                    try conn.handleRecv(gpa, ring, cqe);
                },

                .send => {
                    const conn: *Connection = @alignCast(@fieldParentPtr("send_c", c));
                    defer conn.maybeDestroy(gpa);
                    try conn.handleSend(ring, cqe);
                },

                .close => {
                    const conn: *Connection = @alignCast(@fieldParentPtr("close_c", c));
                    defer conn.maybeDestroy(gpa);
                    switch (cqe.err()) {
                        .SUCCESS => log.debug("connection closed: fd={d}, ring_fd={d}", .{ conn.fd, ring.fd }),
                        else => log.err("connection closed with error: {}", .{cqe.err()}),
                    }
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

fn getSqeWithRetry(ring: *IoUring, retries: u6) error{SubmissionQueueFull}!*linux.io_uring_sqe {
    var i: usize = 0;
    while (i < retries) : (i += 1) {
        const sqe = ring.get_sqe() catch {
            _ = ring.submit() catch {};
            continue;
        };
        return sqe;
    }
    @panic("SubmissionQueueFull");
    // return error.SubmissionQueueFull;
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
    ring: *IoUring,
    cqe: linux.io_uring_cqe,
) CallbackError!void {
    switch (cqe.err()) {
        .SUCCESS => {},
        .INVAL => @panic("invalid submission"),
        else => unreachable,
    }

    const conn = try gpa.create(Connection);
    try conn.init(
        gpa,
        ring,
        cqe.res,
    );
    log.info("connection accepted: fd={d}, ring_fd={d}", .{ cqe.res, ring.fd });
}

pub fn getBufferFromCqe(bg: *IoUring.BufferGroup, cqe: linux.io_uring_cqe) error{NoBufferSelected}![]u8 {
    return @errorCast(bg.get_cqe(cqe));
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

    pub fn init(self: *Connection, gpa: Allocator, ring: *IoUring, fd: posix.socket_t) CallbackError!void {
        self.* = .{
            .arena = .init(gpa),
            .fd = fd,
            .response = undefined,
        };

        self.response = .{ .arena = self.arena.allocator() };

        {
            // Prep multishot recv
            var sqe = try getSqeWithRetry(ring, 16);
            sqe.prep_recv(self.fd, &self.buf, 0);
            // sqe.prep_rw(.RECV, fd, 0, 0, 0);
            // sqe.flags |= linux.IOSQE_BUFFER_SELECT;
            // sqe.buf_index = id;
            sqe.user_data = @intFromPtr(&self.recv_c);
            // sqe.ioprio |= linux.IORING_RECV_MULTISHOT;

            self.active += 1;
            self.recv_c.active = true;
        }
    }

    pub fn maybeDestroy(self: *Connection, gpa: Allocator) void {
        if (self.active > 0) return;
        const fd = self.fd;
        self.deinit();
        gpa.destroy(self);
        log.debug("connection destroyed: fd={d}", .{fd});
    }

    pub fn deinit(self: *Connection) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn close(self: *Connection, ring: *IoUring) !void {
        if (self.recv_c.active) {
            // Cancel downstream fd requests
            const sqe = try getSqeWithRetry(ring, 16);
            self.active += 1;
            // Cancel any requests for this fd
            sqe.prep_cancel_fd(self.fd, linux.IORING_ASYNC_CANCEL_ALL);
            sqe.user_data = @intFromPtr(&self.cancel_c);
            self.cancel_c.active = true;
        }
        // Close downstream
        const sqe = try getSqeWithRetry(ring, 16);
        self.active += 1;
        sqe.prep_close(self.fd);
        sqe.user_data = @intFromPtr(&self.close_c);
        self.close_c.active = true;
    }

    pub fn handleRecv(
        self: *Connection,
        gpa: Allocator,
        ring: *IoUring,
        cqe: linux.io_uring_cqe,
    ) !void {
        self.active -= 1;
        self.recv_c.active = false;
        if (cqe.res == 0) {
            log.err("recv returned 0", .{});
            return self.close(ring);
        }

        switch (cqe.err()) {
            .SUCCESS => {},
            .CANCELED => return,
            .CONNRESET => {
                // This fd was closed by the peer
                try self.close(ring);
                return;
            },
            else => {
                log.err("recv error: {}", .{cqe.err()});
                return self.close(ring);
            },
        }

        // // Get the buffer from the buffer group
        // const buf = getBufferFromCqe(bg, cqe) catch {
        //     // If we have no buffer selected, it's probably because the peer closed the connection.
        //     // This function only returns one error but we assert here because other errors could be
        //     // added in the future and we need to know that
        //     log.err("get buffer from cqe error", .{});
        //     return self.close(ring);
        // };
        // defer bg.put_cqe(cqe) catch unreachable; // Would have errored on previous call

        try self.request.appendSlice(gpa, self.buf[0..@intCast(cqe.res)]);

        if (self.request.head_len) |_| {
            // Call the handler
            log.debug("parsed head", .{});
            var arena: std.heap.ArenaAllocator = .init(gpa);
            self.response = .{ .arena = arena.allocator() };
            try handler(self.request, &self.response);
            try self.prepResponse();
            try self.send(ring);
        }
        var sqe = try getSqeWithRetry(ring, 16);
        self.active += 1;
        sqe.prep_recv(self.fd, &self.buf, 0);
        // sqe.prep_rw(.RECV, fd, 0, 0, 0);
        // sqe.flags |= linux.IOSQE_BUFFER_SELECT;
        // sqe.buf_index = id;
        sqe.user_data = @intFromPtr(&self.recv_c);
    }

    fn reset(self: *Connection) void {
        _ = self.arena.reset(.free_all);
        self.request = .{};
        self.response = .{ .arena = self.arena.allocator() };
    }

    fn prepResponse(self: *Connection) !void {
        var headers = &self.write_buf;
        const resp = self.response;
        const status = resp.status orelse .ok;

        if (status.phrase()) |phrase| {
            try headers.writer(self.arena.allocator()).print(
                "HTTP/1.1 {d} {s}\r\n",
                .{ @intFromEnum(status), phrase },
            );
        } else {
            try headers.writer(self.arena.allocator()).print(
                "HTTP/1.1 {d}\r\n",
                .{@intFromEnum(status)},
            );
        }

        try headers.writer(self.arena.allocator()).print(
            "Content-Length: {d}\r\n",
            .{resp.body.items.len},
        );
        // TODO: sniff content type
        try headers.writer(self.arena.allocator()).print(
            "Content-Type: {d}\r\n",
            .{"text/plain"},
        );

        var iter = resp.headers.iterator();
        while (iter.next()) |header| {
            try headers.writer(self.arena.allocator()).print(
                "{s}: {s}\r\n",
                .{ header.key_ptr.*, header.value_ptr.* },
            );
        }
        try headers.appendSlice(self.arena.allocator(), "\r\n");

        try headers.appendSlice(self.arena.allocator(), self.response.body.items);
        self.response.body.clearRetainingCapacity();
    }

    fn responseSendDone(self: *Connection) bool {
        return self.write_buf.items.len == 0 and
            self.response.body.items.len == 0;
    }

    fn send(self: *Connection, ring: *IoUring) !void {
        if (self.responseSendDone()) {
            self.reset();
            return;
        }

        if (self.send_c.active) return;

        const bytes = if (self.write_buf.items.len > 0)
            self.write_buf.items
        else
            self.response.body.items;

        const sqe = try getSqeWithRetry(ring, 16);
        self.active += 1;
        // sqe.prep_send(self.fd, bytes, 0);
        sqe.prep_send_zc(self.fd, bytes, 0, 0);
        sqe.user_data = @intFromPtr(&self.send_c);
        self.send_c.active = true;
    }

    pub fn handleSend(self: *Connection, ring: *IoUring, cqe: linux.io_uring_cqe) CallbackError!void {
        log.info("send completion: flags={d}", .{cqe.flags});
        if (cqe.flags & linux.IORING_CQE_F_MORE != 0) {
            if (self.write_buf.items.len > 0) {
                self.write_buf.replaceRangeAssumeCapacity(0, @intCast(cqe.res), "");
            } else {
                self.response.body.replaceRangeAssumeCapacity(0, @intCast(cqe.res), "");
            }
            return;
        }
        self.active -= 1;
        self.send_c.active = false;
        // if (self.active == 0) {
        //     try self.close(ring);
        // }
        switch (cqe.err()) {
            .SUCCESS => {},
            else => {
                log.err("send error: {}", .{cqe.err()});
                return self.close(ring);
            },
        }

        // Trigger a send again, this will return if we don't have anything to send
        try self.send(ring);
    }
};

pub const Request = struct {
    /// The raw bytes of the request
    bytes: std.ArrayListUnmanaged(u8) = .empty,

    /// Length of the head section. When null, we have not received enough bytes to finish the head
    head_len: ?usize = null,

    /// The parsed head section. Only valid if head_len is not null
    head: http.Server.Request.Head = undefined,

    /// Body of the request. This is null if we haven't received the full body, or there is no body
    body: ?[]const u8 = null,

    pub fn reset(self: *Request) void {
        self.* = .{};
    }

    // add the slice to the internal buffer, attempt to parse a Head
    pub fn appendSlice(self: *Request, gpa: Allocator, bytes: []const u8) !void {
        try self.bytes.appendSlice(gpa, bytes);

        var hp: HeadParser = .{};
        const head_len = hp.feed(self.bytes.items);

        // Not enough bytes to finish the head
        if (head_len > self.bytes.items.len) return;

        // We have enough bytes to finish the head
        self.head_len = head_len;
        self.head = try .parse(bytes[0..head_len]);

        if (self.head.content_length) |cl| {
            // If we have a content length, we'll set the body if we have enough bytes
            if (head_len + cl == self.bytes.items.len) self.body = self.bytes.items[head_len..];
        }
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
    // try resp.any().writeAll("Hey");
}
