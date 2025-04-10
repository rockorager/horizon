const Server = @This();

const std = @import("std");
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
workers: []Worker,
next_ring: usize = 0,

fd: posix.fd_t,
addr: net.Address,

accept_task: ?*io.Task = null,
shutdown_signal: u6 = 0,
stop_pipe: [2]posix.fd_t,

timeout: Timeouts,

shutdown_timeout: u8,

state: State = .init,

ring: *io.Ring,

exited_workers: u16 = 0,

pub const Options = struct {
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

const State = enum {
    init,
    accepting,
    closing,
};

pub fn init(self: *Server, gpa: Allocator, opts: Options) !void {
    // Set the number of open files to system max
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
        .gpa = gpa,
        .workers = try gpa.alloc(Worker, worker_count),
        .threads = try gpa.alloc(std.Thread, worker_count),
        .fd = fd,
        .addr = addr,
        .stop_pipe = try posix.pipe2(.{}),
        .shutdown_signal = opts.shutdown_signal,
        .timeout = .{
            .read_header = opts.read_header_timeout,
            .read_body = opts.read_body_timeout,
            .idle = opts.idle_timeout,
            .write = opts.write_timeout,
        },
        .shutdown_timeout = opts.shutdown_timeout,
        .ring = undefined,
    };

    try posix.bind(fd, &self.addr.any, addr.getOsSockLen());

    var sock_len = addr.getOsSockLen();
    try posix.getsockname(fd, &self.addr.any, &sock_len);
}

pub fn listenAndServe(self: *Server, ioring: *io.Ring, handler: hz.Handler) !void {
    self.ring = ioring;
    try posix.listen(self.fd, 64);
    self.accept_task = try ioring.accept(self.fd, self, onTaskCompletion);
    self.state = .accepting;
    for (self.workers, 0..) |*worker, i| {
        self.threads[i] = try std.Thread.spawn(
            .{},
            Worker.run,
            .{ worker, self.gpa, self, self.timeout, handler },
        );
    }
    _ = try ioring.poll(self.stop_pipe[0], posix.POLL.IN, self, onTaskCompletion);
}

pub fn deinit(self: *Server, gpa: Allocator) void {
    for (self.workers) |*worker| {
        worker.deinit();
    }

    posix.close(self.stop_pipe[0]);
    posix.close(self.stop_pipe[1]);

    gpa.free(self.workers);
    gpa.free(self.threads);
}

fn onTaskCompletion(ring: *io.Ring, task: *io.Task, result: io.Result) anyerror!void {
    const self = task.ptrCast(Server);
    switch (self.state) {
        .init => unreachable,

        .accepting => {
            switch (result) {
                .accept => {
                    if (task.state != .in_flight) {
                        // Requeue accept if it returned
                        self.accept_task = try ring.accept(self.fd, self, onTaskCompletion);
                    }

                    const fd = result.accept catch |err| {
                        switch (err) {
                            error.Canceled => {},
                            else => log.err("accept error: {}", .{err}),
                        }
                        return;
                    };

                    // Send the accepted fd to a worker
                    const target_task = try ring.getTask();
                    const target = self.nextWorker();
                    target_task.* = .{
                        .userdata = target,
                        .callback = Worker.onTaskCompletion,
                        .req = task.req,
                    };

                    _ = try ring.msgRing(&target.ring, target_task, @intCast(fd), self, onMsgRing);
                },

                .poll => { // shutdown
                    var buf: [8]u8 = undefined;
                    _ = posix.read(self.stop_pipe[0], &buf) catch 0;
                    const accept_task = self.accept_task orelse unreachable;
                    try accept_task.cancel(ring, self, onTaskCompletion);
                    self.state = .closing;

                    // Set our shutdown timer
                    _ = try ring.timer(.{ .sec = self.shutdown_timeout }, self, onTaskCompletion);

                    // Message each ring to shut down
                    for (self.workers) |*worker| {
                        const target_task = try ring.getTask();
                        target_task.* = .{
                            .userdata = worker,
                            .callback = Worker.onTaskCompletion,
                            .req = .usermsg,
                        };

                        _ = try ring.msgRing(
                            &worker.ring,
                            target_task,
                            @intFromEnum(UserMsg.quit),
                            self,
                            onMsgRing,
                        );
                    }
                },

                else => unreachable,
            }
        },

        .closing => {
            switch (result) {
                .accept => {
                    // If for some reason we get a valid fd after issuing our cancelation, we just
                    // close it
                    const fd = result.accept catch return;
                    posix.close(fd);
                },

                .timer => try ring.cancelAll(),

                .cancel => {
                    _ = result.cancel catch |err| {
                        log.err("cancel error: {}", .{err});
                    };
                },

                .usermsg => |v| {
                    const msg: UserMsg = @enumFromInt(v);
                    switch (msg) {
                        .worker_shutdown => {
                            self.exited_workers += 1;
                            if (self.exited_workers == self.workers.len) {
                                try self.ring.cancelAll();
                                for (self.threads) |thread| {
                                    thread.join();
                                }
                            }
                        },

                        else => unreachable,
                    }
                },

                else => unreachable,
            }
        },
    }
}

fn onMsgRing(_: *io.Ring, task: *io.Task, result: io.Result) anyerror!void {
    assert(result == .msg_ring);
    _ = result.msg_ring catch |err| {
        log.err("msg_ring error: {}", .{err});

        const target_task = task.req.msg_ring.task;
        switch (target_task.req) {
            // If we failed to send an accept, we close the fd
            .accept => posix.close(task.req.msg_ring.result),
            else => unreachable,
        }
    };
}

fn nextWorker(self: *Server) *Worker {
    const target = &self.workers[self.next_ring];
    self.next_ring = (self.next_ring + 1) % (self.workers.len);
    return target;
}

pub fn stop(self: *Server) !void {
    _ = try posix.write(self.stop_pipe[1], "q");
}

const Worker = struct {
    gpa: Allocator,
    ring: io.Ring,
    conn_pool: MemoryPool(Connection),
    state: Worker.State,
    timeout: Timeouts,
    handler: hz.Handler,

    server: *Server,

    /// Number of active completions we have on the queue that we want to wait for before gracefully
    /// shutting down
    keep_alive: u16,

    const State = enum {
        running,
        shutting_down,
    };

    fn run(
        self: *Worker,
        gpa: Allocator,
        server: *Server,
        timeout: Timeouts,
        handler: hz.Handler,
    ) !void {
        self.* = .{
            .gpa = gpa,
            .ring = try server.ring.initChild(64),
            .conn_pool = .empty,
            .state = .running,
            .timeout = timeout,
            .keep_alive = 0,
            .handler = handler,
            .server = server,
        };

        try self.ring.run(.forever);
    }

    fn deinit(self: *Worker) void {
        self.conn_pool.deinit(self.gpa);
        self.ring.deinit();
    }

    fn onTaskCompletion(ring: *io.Ring, task: *io.Task, result: io.Result) anyerror!void {
        const self = task.ptrCast(Worker);
        state: switch (self.state) {
            .running => {
                switch (result) {
                    .accept => {
                        // We only receive accept from msg_ring tasks, and we only allow successful
                        // accepts to be sent
                        const fd = result.accept catch unreachable;
                        const conn = try self.conn_pool.create(self.gpa);

                        try conn.init(self.gpa, self, fd);
                    },

                    .usermsg => |v| {
                        const msg: UserMsg = @enumFromInt(v);
                        switch (msg) {
                            .quit => {
                                ring.run_cond = .until_done;
                                self.state = .shutting_down;
                                continue :state .shutting_down;
                            },

                            else => unreachable,
                        }
                    },

                    else => unreachable,
                }
            },

            .shutting_down => {
                try self.maybeClose();
            },
        }
    }

    fn maybeClose(self: *Worker) Allocator.Error!void {
        if (self.state != .shutting_down or self.keep_alive > 0) return;
        if (self.ring.inflight > 0) {
            try self.ring.cancelAll();
        }

        const target_task = try self.ring.getTask();
        target_task.* = .{
            .userdata = self.server,
            .callback = Server.onTaskCompletion,
            .req = .usermsg,
        };

        _ = try self.ring.msgRing(
            self.server.ring,
            target_task,
            @intFromEnum(UserMsg.worker_shutdown),
            null,
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

        const task = try worker.ring.recv(fd, &self.buf, self, Connection.onTaskCompletion);
        if (worker.timeout.read_header > 0) {
            try task.setDeadline(&worker.ring, .{ .sec = self.deadline });
        }
        self.state = .reading_headers;
    }

    fn deinit(self: *Connection) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn onTaskCompletion(ring: *io.Ring, task: *io.Task, result: io.Result) anyerror!void {
        const self = task.ptrCast(Connection);
        state: switch (self.state) {
            .init => unreachable,

            .reading_headers => {
                assert(task.req == .recv);

                const n = result.recv catch continue :state .close;
                if (n == 0) continue :state .close;

                try self.request.appendSlice(self.arena.allocator(), self.buf[0..n]);

                switch (self.request.receivedHeader()) {
                    // When we receive the full header, we pass it to the handler
                    true => continue :state .handling_request,

                    false => {
                        // We haven't received a full HEAD. prep another recv
                        const new_task = try ring.recv(
                            self.fd,
                            &self.buf,
                            self,
                            Connection.onTaskCompletion,
                        );
                        if (self.worker.timeout.read_header > 0) {
                            try new_task.setDeadline(ring, .{ .nsec = self.deadline });
                        }
                    },
                }
            },

            .handling_request => {
                self.state = .handling_request;

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

            .waiting_send_response => {
                assert(task.req == .write or task.req == .writev);
                const n = switch (task.req) {
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

                continue :state .idle;
            },

            .idle => {
                // We'll go back to init state after reinitializing state
                defer self.state = .reading_headers;

                self.reset();

                const new_task = try ring.recv(self.fd, &self.buf, self, Connection.onTaskCompletion);
                // prep another recv
                if (self.worker.timeout.idle > 0) {
                    self.deadline = std.time.timestamp() + self.worker.timeout.idle;
                    try new_task.setDeadline(ring, .{ .sec = self.deadline });
                }
            },

            .close => {
                self.state = .waiting_for_destruction;
                _ = try ring.close(self.fd, self, Connection.onTaskCompletion);
            },

            .waiting_for_destruction => {
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
                Connection.onTaskCompletion,
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
                    Connection.onTaskCompletion,
                );
            },

            .body_only => blk: {
                const offset = self.written - headers.len;
                const unwritten_body = body[offset..];
                break :blk try self.worker.ring.write(
                    self.fd,
                    unwritten_body,
                    self,
                    Connection.onTaskCompletion,
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

const UserMsg = enum(u16) {
    quit,
    worker_shutdown,
};

test "server" {
    const gpa = std.testing.allocator;
    var ring = try io.Ring.init(gpa, 8);
    defer ring.deinit();

    var server: Server = undefined;

    try server.init(gpa, .{ .workers = 1 });
    defer server.deinit(gpa);

    const TestHandler = struct {
        got_request: bool = false,
        server: *Server,

        fn handler(self: *@This()) hz.Handler {
            return .init(@This(), self);
        }

        pub fn serveHttp(
            ptr: *anyopaque,
            ctx: *hz.Context,
            w: hz.ResponseWriter,
            _: hz.Request,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.got_request = true;
            w.any().print("hello world", .{}) catch unreachable;
            try ctx.sendResponse();
            try self.server.stop();
        }
    };

    var handler: TestHandler = .{ .server = &server };
    try server.listenAndServe(&ring, handler.handler());

    _ = try std.Thread.spawn(.{}, testConnection, .{ gpa, server.addr });
    try ring.run(.until_done);

    try std.testing.expect(handler.got_request);
}

fn testConnection(gpa: Allocator, addr: net.Address) !void {
    var client: std.http.Client = .{ .allocator = gpa };
    defer client.deinit();

    _ = try client.fetch(.{ .location = .{
        .uri = .{
            .scheme = "http",
            .host = .{ .raw = "localhost" },
            .port = addr.getPort(),
        },
    } });
}
