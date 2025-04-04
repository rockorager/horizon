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

pub const Worker = struct {
    should_quit: bool,
    ring: io.Ring,
    handler: Handler,

    /// Number of active completions we have on the queue that we want to wait for before gracefully
    /// shutting down
    keep_alive: u8,

    fn serve(self: *Worker, gpa: Allocator, server: *Server) !void {
        self.ring = try server.ring.initChild();
        while (!self.should_quit) {
            try self.ring.submitAndWait();

            while (self.ring.nextCompletion()) |cqe| {
                try self.handleCqe(gpa, cqe);
            }
        }

        while (self.keep_alive > 0) {
            try self.ring.submitAndWait();

            while (self.ring.nextCompletion()) |cqe| {
                try self.handleCqe(gpa, cqe);
            }
        }

        // Notify the main thread we have exited
        try self.ring.msgRing(server.ring, 0, @intFromEnum(Server.Op.thread_exit), 0);
        try self.ring.submit();
    }

    fn handleCqe(
        self: *Worker,
        gpa: Allocator,
        cqe: io.Completion,
    ) !void {
        const c: *Completion = @ptrFromInt(cqe.userdata);
        switch (c.parent) {
            .server => switch (c.op) {
                .accept => try self.onAccept(gpa, cqe),
                .stop => self.should_quit = true,
                else => unreachable,
            },
            .connection => {
                switch (c.op) {
                    .recv => {
                        const conn: *Connection = @alignCast(@fieldParentPtr("recv_c", c));
                        defer conn.maybeDestroy(gpa);
                        try conn.onRecv(self, cqe);
                    },

                    .send => {
                        const conn: *Connection = @alignCast(@fieldParentPtr("send_c", c));
                        defer conn.maybeDestroy(gpa);
                        try conn.onSendCompletion(self, cqe);
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

    fn onAccept(self: *Worker, gpa: Allocator, cqe: io.Completion) !void {
        const result = cqe.unwrap() catch {
            log.err("accept error: {}", .{cqe.err()});
            @panic("accept error");
        };

        const conn = try gpa.create(Connection);
        try conn.init(gpa, self, result);
    }
};

pub const Server = struct {
    ring: io.Ring,

    threads: []std.Thread,
    workers: []Worker,
    next_ring: usize = 0,

    fd: posix.fd_t,
    addr: net.Address,

    shutdown_signal: u6 = 0,
    stop_pipe: [2]posix.fd_t,

    /// Stable completion pointer for messaging threads
    accept_c: Completion = .{ .parent = .server, .op = .accept },

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

        /// If set, the server will listen for this signal to initiate a graceful shutdown. The
        /// first signal will trigger a graceful shutdown. The second will initiate an immediate
        /// shutdown
        shutdown_signal: u6 = 0,
    };

    pub const Op = enum {
        accept,
        cancel_accept,
        msg_ring,
        stop,
        thread_exit,
        timeout,
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
            .workers = try gpa.alloc(Worker, worker_count),
            .threads = try gpa.alloc(std.Thread, worker_count),
            .fd = fd,
            .addr = addr,
            .stop_pipe = try posix.pipe2(.{}),
            .shutdown_signal = opts.shutdown_signal,
        };

        try posix.bind(fd, &self.addr.any, addr.getOsSockLen());
        try posix.listen(fd, 64);

        var sock_len = addr.getOsSockLen();
        try posix.getsockname(fd, &self.addr.any, &sock_len);
    }

    /// Queues a stop of the server
    pub fn deinit(self: *Server, gpa: Allocator) void {
        for (self.workers) |*worker| {
            worker.ring.deinit();
        }
        self.ring.deinit();

        gpa.free(self.workers);
        gpa.free(self.threads);
    }

    pub fn serve(
        self: *Server,
        gpa: Allocator,
        handler: Handler,
    ) !void {
        // If we have a shutdown_signal, we have to register it before spawning threads. Each thread
        // inherits the signal mask
        var sfd: ?posix.fd_t = null;
        if (self.shutdown_signal > 0) {
            // Listen for the shutdown signal
            sfd = try self.ring.signalfd(self.shutdown_signal, @intFromEnum(Op.stop));
            log.debug("sfd={d}", .{sfd.?});
        }

        for (self.workers, 0..) |*worker, i| {
            worker.* = .{
                .should_quit = false,
                .ring = undefined,
                .handler = handler,
                .keep_alive = 0,
            };
            worker.handler = handler;
            self.threads[i] = try std.Thread.spawn(.{}, Worker.serve, .{ worker, gpa, self });
        }

        // Start listening for stop signals from the stop pipe
        try self.ring.poll(self.stop_pipe[0], @intFromEnum(Op.stop));

        // Start accepting connections
        try self.ring.accept(self.fd, @intFromEnum(Op.accept));

        var should_quit: bool = false;
        while (!should_quit) {
            try self.ring.submitAndWait();

            while (self.ring.nextCompletion()) |cqe| {
                const op: Op = @enumFromInt(cqe.userdata);
                switch (op) {
                    .accept => try self.acceptAndSend(cqe),

                    .msg_ring => _ = cqe.unwrap() catch |err|
                        log.err("msg not sent to ring: {}", .{err}),

                    // When we receive the stop signal, we cancel the accept
                    .stop => {
                        if (sfd) |fd| {
                            var buf: [@sizeOf(posix.siginfo_t)]u8 = undefined;
                            _ = try posix.read(fd, &buf);
                        }
                        try self.ring.cancel(
                            @intFromEnum(Op.accept),
                            @intFromEnum(Op.cancel_accept),
                        );
                    },

                    // When we receive the cancel accept response, we exit the loop
                    .cancel_accept => should_quit = true,

                    // We don't ever receive this response in this loop
                    .thread_exit => unreachable,

                    .timeout => unreachable,
                }
            }
        }

        try self.cleanUp(sfd);
    }

    fn cleanUp(self: *Server, sfd: ?posix.fd_t) !void {
        // Reregister the shutdown signal so we can do a quick shutdown
        // var sfd: ?posix.fd_t = null;
        // if (self.shutdown_signal > 0) {
        //     sfd = try self.ring.signalfd(self.shutdown_signal, @intFromEnum(Op.stop));
        // }
        // Shutdown
        //
        // 1. Set a timeout for total shutdown time
        // 2. Notify all threads of shutdown
        // 3. Wait for signals from threads that they have closed
        // 4. If we get another stop signal, detach threads and exit
        // 5. If we got all our thread exit signals, we close cleanly

        var stop_c: Completion = .{ .parent = .server, .op = .stop };

        // 60 second timeout to gracefully close
        try self.ring.timer(60, @intFromEnum(Op.timeout));

        for (self.workers) |worker| {
            try self.ring.msgRing(worker.ring, 0, @intFromPtr(&stop_c), @intFromEnum(Op.msg_ring));
        }

        var exited_threads: usize = 0;
        while (exited_threads < self.workers.len) {
            try self.ring.submitAndWait();

            while (self.ring.nextCompletion()) |cqe| {
                const op: Op = @enumFromInt(cqe.userdata);
                switch (op) {
                    .accept => unreachable,

                    .msg_ring => _ = cqe.unwrap() catch |err|
                        log.err("msg not sent to ring: {}", .{err}),

                    // If we receive another stop signal, we return immediately
                    .stop => {
                        if (sfd) |fd| {
                            var buf: [@sizeOf(posix.siginfo_t)]u8 = undefined;
                            _ = try posix.read(fd, &buf);
                            posix.close(fd);
                        }
                        return;
                    },

                    // When we receive the cancel accept response, we exit the loop
                    .cancel_accept => unreachable,

                    // We don't ever receive this response in this loop
                    .thread_exit => exited_threads += 1,

                    .timeout => return,
                }
            }
        }

        for (self.threads) |thread| {
            thread.join();
        }
    }

    /// Accept a connection and send it to a thread to work on it
    fn acceptAndSend(
        self: *Server,
        cqe: io.Completion,
    ) CallbackError!void {
        const result = cqe.unwrap() catch |err| {
            switch (err) {
                error.Canceled => {},
                else => log.err("accept unwrap: {}", .{err}),
            }
            return;
        };

        const target = self.workers[self.next_ring];
        self.next_ring = (self.next_ring + 1) % (self.workers.len);

        if (!cqe.flags.has_more) {
            // Multishot accept will return if it wasn't able to accept the next connection. Requeue it
            try self.ring.accept(self.fd, @intFromEnum(Op.accept));
        }

        // Send the fd to another thread for handling
        try self.ring.msgRing(target.ring, result, @intFromPtr(&self.accept_c), @intFromEnum(Op.msg_ring));
    }

    pub fn stop(self: *Server) !void {
        _ = try posix.write(self.stop_pipe[1], "q");
    }
};

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
        stop,
        signal,
    },

    /// If the completion has been scheduled
    active: bool = false,
};

const CallbackError = Allocator.Error || error{
    /// Returned when a recv didn't have a buffer selected
    NoBufferSelected,
    SubmissionQueueFull,
};

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

    was_idle: bool,

    pub fn init(
        self: *Connection,
        gpa: Allocator,
        worker: *Worker,
        fd: posix.socket_t,
    ) CallbackError!void {
        self.* = .{
            .arena = .init(gpa),
            .fd = fd,
            .response = undefined,
            .vecs = undefined,
            .was_idle = false,
        };

        self.response = .{ .arena = self.arena.allocator() };

        try worker.ring.recv(self.fd, &self.buf, &self.recv_c);

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
            try ring.cancel(@intFromPtr(&self.recv_c), @intFromPtr(&self.cancel_c));
            self.active += 1;
            self.cancel_c.active = true;
        }
        // Close downstream
        try ring.close(self.fd, &self.close_c);
        self.active += 1;
        self.close_c.active = true;
    }

    pub fn onRecv(
        self: *Connection,
        worker: *Worker,
        cqe: io.Completion,
    ) !void {
        self.active -= 1;
        self.recv_c.active = false;
        if (cqe.result == 0) {
            // Peer closed connection
            return self.close(&worker.ring);
        }

        const result = cqe.unwrap() catch |err| {
            switch (err) {
                error.Canceled => return self.close(&worker.ring),
                error.ConnectionResetByPeer => return self.close(&worker.ring),
                else => {
                    log.err("recv error: {}", .{cqe.err()});
                    return self.close(&worker.ring);
                },
            }
        };

        try self.request.appendSlice(self.arena.allocator(), self.buf[0..result]);

        if (!self.request.receivedHeader()) {
            // We haven't received a full HEAD. prep another recv
            try worker.ring.recv(self.fd, &self.buf, &self.recv_c);
            self.active += 1;
            self.recv_c.active = true;
            return;
        }

        // Keep the worker alive since we are now handling this request
        worker.keep_alive += 1;
        // We've received a full HEAD, call the handler now
        try worker.handler.serveHttp(self.response.responseWriter(), self.request);
        try self.prepareHeader();
        try self.send(worker);
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

        if (status.phrase()) |phrase| {
            writer.print("HTTP/1.1 {d} {s}\r\n", .{ @intFromEnum(status), phrase }) catch unreachable;
        } else {
            writer.print("HTTP/1.1 {d}\r\n", .{@intFromEnum(status)}) catch unreachable;
        }

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

    fn send(self: *Connection, worker: *Worker) !void {
        // Pending headers, no body
        if (self.written < self.write_buf.items.len and self.response.body.items.len == 0) {
            try worker.ring.write(self.fd, self.write_buf.items[self.written..], &self.send_c);
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

            try worker.ring.writev(self.fd, &self.vecs, &self.send_c);
            self.active += 1;
            self.send_c.active = true;

            return;
        }

        // Pending body
        const offset = self.written - self.write_buf.items.len;
        const body = self.response.body.items[offset..];
        self.active += 1;
        try worker.ring.write(self.fd, body, &self.send_c);
        self.send_c.active = true;
    }

    fn onSendCompletion(self: *Connection, worker: *Worker, cqe: io.Completion) CallbackError!void {
        self.active -= 1;
        self.send_c.active = false;
        switch (cqe.err()) {
            .SUCCESS => {},
            else => {
                // Response send error, we can let the worker exit if it wants to
                worker.keep_alive -= 1;
                log.err("send error: {}", .{cqe.err()});
                return self.close(&worker.ring);
            },
        }

        self.written += @intCast(cqe.result);

        switch (self.responseComplete()) {
            // If the response didn't finish sending, try again
            false => return self.send(worker),

            // Response sent. Decide if we should close the connection or keep it alive
            true => {
                // Response has been fully sent, we could exit gracefully
                worker.keep_alive -= 1;

                // If the worker is quitting, we can close this connection
                if (worker.should_quit or !self.request.keepAlive()) {
                    return self.close(&worker.ring);
                }

                self.reset();
                // prep another recv
                try worker.ring.recv(self.fd, &self.buf, &self.recv_c);
                self.active += 1;
            },
        }
    }
};

pub const Request = struct {
    /// The raw bytes of the request. This may not be the full request - handlers are called when we
    /// have received the headers. At that point, callers can instruct the response to read the full
    /// body. In this case, the callback will be called again when the full body has been read
    bytes: std.ArrayListUnmanaged(u8) = .empty,

    // add the slice to the internal buffer
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

pub fn errorResponse(
    w: ResponseWriter,
    status: http.Status,
    comptime format: []const u8,
    args: anytype,
) anyerror!void {
    w.setStatus(status);
    // Clear the Content-Length header
    try w.setHeader(strings.content_length, null);
    // Set content type
    try w.setHeader(strings.content_type, "text/plain");
    // Print the body
    try w.any().print(format, args);
}

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

test Server {
    const addr: net.Address = try .parseIp4("127.0.0.1", 0);
    const opts: Server.Options = .{ .addr = addr };
    var server: Server = undefined;

    const TestHandler = struct {
        got_request: bool = false,

        fn handler(self: *@This()) Handler {
            return .init(@This(), self);
        }

        pub fn serveHttp(ptr: *anyopaque, w: ResponseWriter, _: Request) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.got_request = true;
            w.any().print("hello world", .{}) catch unreachable;
        }
    };

    var wg: std.Thread.WaitGroup = .{};
    wg.start();
    var handler: TestHandler = .{};
    const thread = try std.Thread.spawn(.{}, spawnTestServer, .{
        &server,
        &wg,
        opts,
        std.testing.allocator,
        handler.handler(),
    });
    // Wait until server is init'd so we have it's address
    wg.wait();

    const stream = try std.net.tcpConnectToAddress(server.addr);
    defer stream.close();
    _ = try stream.write("GET / HTTP/1.1\r\nConnection: close\r\n\r\n");

    var buf: [4096]u8 = undefined;
    const n = try stream.read(&buf);

    try std.testing.expect(handler.got_request);
    try std.testing.expectStringStartsWith(buf[0..n], "HTTP/1.1 200");

    try server.stop();
    thread.join();
}

fn spawnTestServer(server: *Server, wg: *std.Thread.WaitGroup, opts: Server.Options, gpa: Allocator, handler: Handler) !void {
    {
        defer wg.finish();
        try server.init(std.testing.allocator, opts);
    }
    defer server.deinit(std.testing.allocator);

    try server.serve(gpa, handler);
}
