const Server = @This();

const std = @import("std");
const builtin = @import("builtin");
const io = @import("io");
const hz = @import("main.zig");
const sniff = @import("sniff.zig");

const Allocator = std.mem.Allocator;
const MemoryPool = @import("pool.zig").MemoryPool;
const net = std.net;
const posix = std.posix;

const assert = std.debug.assert;
const log = std.log.scoped(.horizon);

gpa: Allocator,
threads: []std.Thread,
worker: Worker,
workers: []Worker,

addr: net.Address,

stop_pipe: [2]posix.fd_t,

timeout: Timeouts,

shutdown_timeout: u8,

ring: *io.Runtime,

exited_workers: u16 = 0,

poll_task: ?*io.Task = null,
shutdown_timer: ?*io.Task = null,

pub const Options = struct {
    /// Address to listen on
    addr: ?net.Address = null,

    /// Number of worker threads to spawn. If null, we will spawn at least 1 worker, and up to
    /// threadcount - 1 workers. We require one worker because one thread is exclusively used
    /// for the accept loop. Another is used to handle requests. We always need at least 2, but
    /// no more than the core count
    workers: ?u16 = null,

    /// Timeout from first connection, to when we must receive all headers. A value of 0 is no
    /// timeout
    read_header_timeout: u8 = 10,

    /// Timeout from when we have read headers to when we should have read the full body
    read_body_timeout: u8 = 10,

    /// Timeout from end of response to next request on a keep-alive connection
    idle_timeout: u8 = 60,

    /// Timeout to complete the write of the response
    write_timeout: u8 = 20,

    /// Number of seconds to perform a graceful shutdown before exiting. This is the amount of
    /// time to let each worker finish all existing connections, no new connections will be
    /// accepted
    shutdown_timeout: u8 = 60,
};

const Timeouts = struct {
    read_header: u8,
    read_body: u8,
    idle: u8,
    write: u8,
};

pub fn init(self: *Server, gpa: Allocator, opts: Options) !void {
    // Set the number of open files to system max
    const limit = try posix.getrlimit(posix.rlimit_resource.NOFILE);
    try posix.setrlimit(posix.rlimit_resource.NOFILE, .{ .cur = limit.max, .max = limit.max });

    const addr = if (opts.addr) |a|
        a
    else
        net.Address.parseIp4("127.0.0.1", 8080) catch unreachable;

    // We always subtract one because the main thread counts as a worker too
    const worker_count: usize = if (opts.workers) |w| w -| 1 else blk: {
        const cpu_count = std.Thread.getCpuCount() catch 1;
        break :blk cpu_count -| 1;
    };

    self.* = .{
        .gpa = gpa,
        .worker = undefined,
        .workers = try gpa.alloc(Worker, worker_count),
        .threads = try gpa.alloc(std.Thread, worker_count),
        .addr = addr,
        .stop_pipe = try posix.pipe2(.{ .CLOEXEC = true }),
        .timeout = .{
            .read_header = opts.read_header_timeout,
            .read_body = opts.read_body_timeout,
            .idle = opts.idle_timeout,
            .write = opts.write_timeout,
        },
        .shutdown_timeout = opts.shutdown_timeout,
        .ring = undefined,
        .poll_task = undefined,
    };
}

const Msg = enum {
    main_worker_ready,
    shutdown,
    shutdown_timeout,
    msg_ring_fail,
    worker_shutdown,

    fn fromInt(v: u16) Msg {
        return @enumFromInt(v);
    }
};

fn handleMsg(ptr: ?*anyopaque, rt: *io.Runtime, msg: u16, result: io.Result) anyerror!void {
    const self = io.ptrCast(Server, ptr);

    switch (Msg.fromInt(msg)) {
        .main_worker_ready => {
            assert(result == .poll);

            _ = result.poll catch |err| {
                switch (err) {
                    error.Canceled => return,
                    else => {},
                }
                log.err("poll error: {}", .{err});
                return;
            };

            switch (builtin.os.tag) {
                .linux => {
                    const fd = self.worker.ring.pollableFd() catch unreachable;
                    var buf: [8]u8 = undefined;
                    _ = posix.read(fd, &buf) catch |err| {
                        switch (err) {
                            error.WouldBlock => return,
                            else => {},
                        }
                    };
                },
                else => {},
            }

            // Rearm the poll task
            self.poll_task = try rt.poll(
                try self.worker.ring.pollableFd(),
                posix.POLL.IN,
                self,
                @intFromEnum(Msg.main_worker_ready),
                handleMsg,
            );
        },

        .shutdown => {
            var buf: [8]u8 = undefined;
            _ = posix.read(self.stop_pipe[0], &buf) catch 0;

            // Set our shutdown timer
            self.shutdown_timer = try rt.timer(
                .{ .sec = self.shutdown_timeout },
                self,
                @intFromEnum(Msg.shutdown_timeout),
                handleMsg,
            );

            {
                const target_task = try rt.getTask();
                target_task.* = .{
                    .userdata = &self.worker,
                    .msg = @intFromEnum(Worker.Msg.shutdown),
                    .callback = Worker.handleMsg,
                    .req = .usermsg,
                };

                _ = try rt.msgRing(
                    &self.worker.ring,
                    target_task,
                    0,
                    self,
                    @intFromEnum(Msg.msg_ring_fail),
                    handleMsg,
                );
            }

            // Message each worker ring to shut down
            for (self.workers) |*worker| {
                const target_task = try rt.getTask();
                target_task.* = .{
                    .userdata = worker,
                    .msg = @intFromEnum(Worker.Msg.shutdown),
                    .callback = Worker.handleMsg,
                    .req = .usermsg,
                };

                _ = try rt.msgRing(
                    &worker.ring,
                    target_task,
                    0,
                    self,
                    @intFromEnum(Msg.msg_ring_fail),
                    handleMsg,
                );
            }
        },

        .shutdown_timeout => {
            self.shutdown_timer = null;
            if (self.poll_task) |pt| {
                _ = try pt.cancel(rt, null, 0, io.noopCallback);
                self.poll_task = null;
            }
        },

        .msg_ring_fail => {
            _ = result.msg_ring catch |err| {
                log.err("msg_ring error: {}", .{err});
            };
        },

        .worker_shutdown => {
            self.exited_workers += 1;
            if (self.exited_workers == self.workers.len + 1) {
                for (self.threads) |thread| {
                    thread.join();
                }
                if (self.poll_task) |pt| {
                    _ = try pt.cancel(rt, null, 0, io.noopCallback);
                    self.poll_task = null;
                }
                if (self.shutdown_timer) |timer| {
                    _ = try timer.cancel(rt, null, 0, io.noopCallback);
                    self.shutdown_timer = null;
                }
            }
        },
    }

    // We reap and submit our main worker ring here. It's possible that in this function our eventfd
    // won't wake up when we send a msg_ring message. Both of these calls are nonblocking, and we
    // only are in this function if we are shutting down, so we don't care too much about perf
    try self.worker.ring.reapCompletions();
    try self.worker.ring.submit();
}

pub fn listenAndServe(self: *Server, rt: *io.Runtime, handler: hz.Handler) !void {
    self.ring = rt;

    {
        self.worker = try .init(self.gpa, self.timeout, handler, self.addr, self);
        self.worker.ring = try rt.initChild(64);
        try posix.listen(self.worker.fd, 64);
        self.worker.accept_task = try self.worker.ring.accept(
            self.worker.fd,
            &self.worker,
            @intFromEnum(Worker.Msg.new_connection),
            Worker.handleMsg,
        );

        var sock_len = self.addr.getOsSockLen();
        try posix.getsockname(self.worker.fd, &self.addr.any, &sock_len);

        try self.worker.ring.submit();
    }

    for (self.workers, 0..) |*worker, i| {
        worker.* = try .init(self.gpa, self.timeout, handler, self.addr, self);

        self.threads[i] = try std.Thread.spawn(
            .{},
            Worker.run,
            .{ worker, rt },
        );
    }
    // The main server ring needs a callback from the main ioring
    self.poll_task = try rt.poll(
        try self.worker.ring.pollableFd(),
        posix.POLL.IN,
        self,
        @intFromEnum(Server.Msg.main_worker_ready),
        handleMsg,
    );
    _ = try rt.poll(
        self.stop_pipe[0],
        posix.POLL.IN,
        self,
        @intFromEnum(Server.Msg.shutdown),
        handleMsg,
    );
}

pub fn deinit(self: *Server, gpa: Allocator) void {
    self.worker.deinit();
    for (self.workers) |*worker| {
        worker.deinit();
    }

    if (self.poll_task) |pt| {
        _ = pt.cancel(self.ring, null, 0, io.noopCallback) catch {};
        self.poll_task = null;
    }

    posix.close(self.stop_pipe[0]);
    posix.close(self.stop_pipe[1]);

    gpa.free(self.workers);
    gpa.free(self.threads);
}

pub fn stop(self: *Server) !void {
    _ = try posix.write(self.stop_pipe[1], "q");
}

const Worker = struct {
    gpa: Allocator,
    ring: io.Runtime,
    conn_pool: MemoryPool(Connection),
    state: Worker.State,
    timeout: Timeouts,
    handler: hz.Handler,

    shutting_down: bool = false,

    fd: posix.socket_t,
    accept_task: *io.Task,
    server: *Server,

    /// Number of active completions we have on the queue that we want to wait for before gracefully
    /// shutting down
    keep_alive: u16,

    const State = enum {
        running,
        shutting_down,
        done,
    };

    const Msg = enum {
        new_connection,
        shutdown,

        fn fromInt(v: u16) Worker.Msg {
            return @enumFromInt(v);
        }
    };

    fn init(
        gpa: Allocator,
        timeout: Timeouts,
        handler: hz.Handler,
        addr: net.Address,
        server: *Server,
    ) !Worker {
        const flags = posix.SOCK.STREAM | posix.SOCK.CLOEXEC;
        const fd = try posix.socket(addr.any.family, flags, 0);
        try posix.setsockopt(
            fd,
            posix.SOL.SOCKET,
            posix.SO.REUSEADDR,
            &std.mem.toBytes(@as(c_int, 1)),
        );
        try posix.setsockopt(
            fd,
            posix.SOL.SOCKET,
            posix.SO.REUSEPORT,
            &std.mem.toBytes(@as(c_int, 1)),
        );
        try posix.bind(fd, &addr.any, addr.getOsSockLen());

        return .{
            .gpa = gpa,
            .ring = undefined,
            .state = .running,
            .conn_pool = .empty,
            .timeout = timeout,
            .handler = handler,
            .fd = fd,
            .keep_alive = 0,
            .accept_task = undefined,
            .server = server,
        };
    }

    fn run(self: *Worker, main: *io.Runtime) !void {
        self.ring = try main.initChild(64);
        try posix.listen(self.fd, 64);
        self.accept_task = try self.ring.accept(
            self.fd,
            self,
            @intFromEnum(Worker.Msg.new_connection),
            Worker.handleMsg,
        );

        try self.ring.run(.until_done);
    }

    fn deinit(self: *Worker) void {
        self.conn_pool.deinit(self.gpa);
        self.ring.deinit();
    }

    fn handleMsg(ptr: ?*anyopaque, rt: *io.Runtime, msg: u16, result: io.Result) anyerror!void {
        const self = io.ptrCast(Worker, ptr);
        switch (Worker.Msg.fromInt(msg)) {
            .new_connection => {
                const fd = result.accept catch |err| {
                    switch (err) {
                        error.Canceled => {},
                        else => log.err("accept error: {}", .{err}),
                    }
                    return;
                };
                const conn = try self.conn_pool.create(self.gpa);

                try conn.init(self.gpa, self, fd);
            },

            .shutdown => {
                self.state = .shutting_down;
                _ = try self.accept_task.cancel(rt, null, 0, io.noopCallback);
                try self.maybeClose();
            },
        }
    }

    fn maybeClose(self: *Worker) !void {
        if (self.state != .shutting_down or self.keep_alive > 0) return;
        self.state = .done;
        if (self.ring.inflight > 0) {
            try self.ring.cancelAll();
        }

        const target_task = try self.ring.getTask();
        target_task.* = .{
            .userdata = self.server,
            .msg = @intFromEnum(Server.Msg.worker_shutdown),
            .callback = Server.handleMsg,
            .req = .usermsg,
        };

        _ = try self.ring.msgRing(
            self.server.ring,
            target_task,
            0,
            null,
            0,
            io.noopCallback,
        );
    }
};

pub const Connection = struct {
    arena: std.heap.ArenaAllocator,
    worker: *Worker,

    buf: [1024]u8 = undefined,
    fd: posix.socket_t,
    write_buf: std.ArrayListUnmanaged(u8) = .empty,
    vecs: [2]posix.iovec_const = undefined,
    written: usize = 0,

    deadline: i64,

    ctx: hz.Context,
    response: hz.Response,
    request: hz.Request,

    state: Connection.State = .init,

    const State = enum {
        init,
        reading_headers,
        handling_request,
        reading_body,
        waiting_send_response,
        idle,
        close,
        waiting_for_destruction,
    };

    const WriteState = enum {
        headers_only,
        headers_and_body,
        body_only,
    };

    fn init(self: *Connection, gpa: Allocator, worker: *Worker, fd: posix.socket_t) !void {
        self.* = .{
            .arena = .init(gpa),
            .worker = worker,
            .fd = fd,
            .ctx = undefined,
            .response = undefined,
            .request = .{},
            .deadline = std.time.timestamp() + worker.timeout.read_header,
        };

        self.response = .{ .arena = self.arena.allocator() };
        self.ctx = .{
            .arena = self.arena.allocator(),
            .deadline = 0,
            .ring = &worker.ring,
        };

        const task = try worker.ring.recv(
            fd,
            &self.buf,
            self,
            @intFromEnum(Connection.Msg.reading_headers),
            Connection.handleMsg,
        );
        if (worker.timeout.read_header > 0) {
            try task.setDeadline(&worker.ring, .{ .sec = self.deadline });
        }
        self.state = .reading_headers;
    }

    fn deinit(self: *Connection) void {
        self.arena.deinit();
        self.* = undefined;
    }

    const Msg = enum {
        reading_headers,
        reading_body,
        write_response,
        close,
        destroy,

        fn fromInt(v: u16) Connection.Msg {
            return @enumFromInt(v);
        }
    };

    fn handleMsg(ptr: ?*anyopaque, rt: *io.Runtime, msg: u16, result: io.Result) anyerror!void {
        const self = io.ptrCast(Connection, ptr);
        state: switch (Connection.Msg.fromInt(msg)) {
            .reading_headers => {
                assert(result == .recv);

                const n = result.recv catch continue :state .close;
                if (n == 0) continue :state .close;

                try self.request.appendSlice(self.arena.allocator(), self.buf[0..n]);

                switch (self.request.receivedHeader()) {
                    // When we receive the full header, we pass it to the handler
                    true => {
                        // Keep the worker alive since we are now handling this request
                        self.worker.keep_alive += 1;

                        // validate the request
                        if (try self.request.isValid(self.response.responseWriter())) {
                            // Call the handler
                            try self.worker.handler.serveHttp(
                                &self.ctx,
                                self.response.responseWriter(),
                                self.request,
                            );
                        }
                    },

                    false => {
                        // We haven't received a full HEAD. prep another recv
                        const new_task = try rt.recv(
                            self.fd,
                            &self.buf,
                            self,
                            @intFromEnum(Connection.Msg.reading_headers),
                            Connection.handleMsg,
                        );
                        if (self.worker.timeout.read_header > 0) {
                            try new_task.setDeadline(rt, .{ .sec = self.deadline });
                        }
                    },
                }
            },

            .reading_body => {
                assert(result == .recv);

                const n = result.recv catch continue :state .close;
                if (n == 0) continue :state .close;

                try self.request.appendSlice(self.arena.allocator(), self.buf[0..n]);

                try self.readBody();
            },

            .write_response => {
                assert(result == .write or result == .writev);
                const n = switch (result) {
                    .write => result.write catch {
                        // On error we are done and can let the worker softclose if it wants
                        self.worker.keep_alive -= 1;
                        continue :state .close;
                    },
                    .writev => result.writev catch {
                        // On error we are done and can let the worker softclose if it wants
                        self.worker.keep_alive -= 1;
                        continue :state .close;
                    },
                    else => unreachable,
                };

                self.written += n;
                if (!self.responseComplete()) {
                    return self.sendResponse();
                }

                // Response sent. Decide if we should close the connection or keep it alive
                // We also could allow the worker to exit gracefully now
                self.worker.keep_alive -= 1;

                // If the worker is quitting, we can close this connection
                if (self.worker.state == .shutting_down or !self.request.keepAlive()) {
                    continue :state .close;
                }

                // Keep the connection alive
                self.reset();
                const new_task = try rt.recv(
                    self.fd,
                    &self.buf,
                    self,
                    @intFromEnum(Connection.Msg.reading_headers),
                    Connection.handleMsg,
                );
                if (self.worker.timeout.idle > 0) {
                    self.deadline = std.time.timestamp() + self.worker.timeout.idle;
                    try new_task.setDeadline(rt, .{ .sec = self.deadline });
                }
            },

            .close => {
                _ = try rt.close(
                    self.fd,
                    self,
                    @intFromEnum(Connection.Msg.destroy),
                    Connection.handleMsg,
                );
            },

            .destroy => {
                assert(result == .close);
                _ = result.close catch |err| {
                    log.err("close error: {}", .{err});
                };
                const worker = self.worker;
                self.deinit();

                worker.conn_pool.destroy(self);

                // If we are shutting down, and have nothing left to keep us alive we cancel
                // everything in flight
                try worker.maybeClose();
            },
        }
    }

    /// Writes the header into write_buf
    pub fn prepareHeader(self: *Connection) !void {
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

        if (resp.headers.get("Content-Length") == null) {
            writer.print("Content-Length: {d}\r\n", .{resp.body.len()}) catch unreachable;
        }

        if (resp.headers.get("Content-Type") == null) {
            const ct = switch (resp.body) {
                .file => unreachable,
                .static => |s| sniff.detectContentType(s),
                .dynamic => |*d| sniff.detectContentType(d.items),
            };
            writer.print("Content-Type: {s}\r\n", .{ct}) catch unreachable;
        }

        var iter = resp.headers.iterator();
        while (iter.next()) |h| {
            writer.print(
                "{s}: {s}\r\n",
                .{ h.key_ptr.*, h.value_ptr.* },
            ) catch unreachable;
        }

        writer.writeAll("\r\n") catch unreachable;
    }

    /// Prepares a recv request to read the body of the request. If the body is fully read, the
    /// handler is called again
    pub fn readBody(self: *Connection) !void {
        const head_len = self.request.headLen() orelse @panic("TODO");
        const cl = self.request.contentLength() orelse @panic("TODO");

        if (head_len + cl == self.request.bytes.items.len) {
            return self.worker.handler.serveHttp(
                &self.ctx,
                self.response.responseWriter(),
                self.request,
            );
        }

        self.state = .reading_body;

        _ = try self.worker.ring.recv(
            self.fd,
            &self.buf,
            self,
            @intFromEnum(Connection.Msg.reading_body),
            Connection.handleMsg,
        );
    }

    pub fn prepareResponse(self: *Connection) !void {
        // Prepare the header
        try self.prepareHeader();

        // Set the deadline
        if (self.worker.timeout.write > 0) {
            self.deadline = std.time.timestamp() + self.worker.timeout.write;
        }
    }

    pub fn sendResponse(self: *Connection) !void {
        self.state = .waiting_send_response;

        const headers = self.write_buf.items;
        const body = switch (self.response.body) {
            .file => @panic("TODO"),
            .static => |s| s,
            .dynamic => |*d| d.items,
        };

        const wstate: WriteState =
            if (self.written < headers.len and body.len == 0)
                .headers_only
            else if (self.written < headers.len)
                .headers_and_body
            else
                .body_only;

        const new_task = switch (wstate) {
            .headers_only => try self.worker.ring.write(
                self.fd,
                headers[self.written..],
                self,
                @intFromEnum(Connection.Msg.write_response),
                Connection.handleMsg,
            ),

            .headers_and_body => blk: {
                const unwritten = headers[self.written..];
                self.vecs[0] = .{
                    .base = unwritten.ptr,
                    .len = unwritten.len,
                };
                self.vecs[1] = .{
                    .base = body.ptr,
                    .len = body.len,
                };

                break :blk try self.worker.ring.writev(
                    self.fd,
                    &self.vecs,
                    self,
                    @intFromEnum(Connection.Msg.write_response),
                    Connection.handleMsg,
                );
            },

            .body_only => blk: {
                const offset = self.written - headers.len;
                const unwritten_body = body[offset..];
                break :blk try self.worker.ring.write(
                    self.fd,
                    unwritten_body,
                    self,
                    @intFromEnum(Connection.Msg.write_response),
                    Connection.handleMsg,
                );
            },
        };

        if (self.worker.timeout.write > 0) {
            try new_task.setDeadline(&self.worker.ring, .{ .sec = self.deadline });
        }
    }

    fn reset(self: *Connection) void {
        _ = self.arena.reset(.retain_capacity);
        self.request = .{};
        self.write_buf = .empty;
        self.response = .{ .arena = self.arena.allocator() };
        self.written = 0;

        self.ctx.deadline = 0;
    }

    fn responseComplete(self: *Connection) bool {
        return self.written == (self.write_buf.items.len + self.response.body.len());
    }
};

test "server" {
    const gpa = std.testing.allocator;
    var ring = try io.Runtime.init(gpa, 8);
    defer ring.deinit();

    var server: Server = undefined;

    try server.init(gpa, .{ .workers = 1, .shutdown_timeout = 2 });
    defer server.deinit(gpa);

    const TestHandler = struct {
        got_request: bool = false,
        server: *Server,

        fn handler(self: *@This()) hz.Handler {
            return .init(@This(), self);
        }

        pub fn serveHttp(
            ptr: *anyopaque,
            _: *hz.Context,
            w: hz.ResponseWriter,
            _: hz.Request,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.got_request = true;
            try w.any().print("hello world", .{});
            try w.flush();
            try self.server.stop();
        }
    };

    var handler: TestHandler = .{ .server = &server };
    try server.listenAndServe(&ring, handler.handler());

    _ = try std.Thread.spawn(.{}, testConnection, .{ gpa, &server, server.addr });

    try ring.run(.until_done);

    try std.testing.expect(handler.got_request);
}

fn testConnection(gpa: Allocator, server: *Server, addr: net.Address) !void {
    _ = server;
    var client: std.http.Client = .{ .allocator = gpa };
    defer client.deinit();

    _ = try client.fetch(.{
        .location = .{
            .uri = .{
                .scheme = "http",
                .host = .{ .raw = "localhost" },
                .port = addr.getPort(),
            },
        },
    });
}
