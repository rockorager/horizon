const Server = @This();

const std = @import("std");
const builtin = @import("builtin");
const ourio = @import("ourio");
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

io: *ourio.Ring,

exited_workers: u16 = 0,

poll_task: ?*ourio.Task = null,
shutdown_timer: ?*ourio.Task = null,

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
        .io = undefined,
        .poll_task = undefined,
    };
}

const Msg = enum {
    main_worker_ready,
    shutdown,
    shutdown_timeout,
    msg_ring_fail,
    worker_shutdown,
};

fn handleMsg(io: *ourio.Ring, task: ourio.Task) anyerror!void {
    const self = task.userdataCast(Server);
    const result = task.result.?;

    switch (task.msgToEnum(Server.Msg)) {
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
                    const fd = self.worker.io.backend.pollableFd() catch unreachable;
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
            self.poll_task = try io.poll(
                try self.worker.io.backend.pollableFd(),
                posix.POLL.IN,
                .{
                    .ptr = self,
                    .msg = @intFromEnum(Msg.main_worker_ready),
                    .cb = handleMsg,
                },
            );
        },

        .shutdown => {
            var buf: [8]u8 = undefined;
            _ = posix.read(self.stop_pipe[0], &buf) catch 0;

            // Set our shutdown timer
            self.shutdown_timer = try io.timer(
                .{ .sec = self.shutdown_timeout },
                .{
                    .ptr = self,
                    .msg = @intFromEnum(Msg.shutdown_timeout),
                    .cb = handleMsg,
                },
            );

            {
                const target_task = try io.getTask();
                target_task.* = .{
                    .userdata = &self.worker,
                    .msg = @intFromEnum(Worker.Msg.shutdown),
                    .callback = Worker.handleMsg,
                    .req = .usermsg,
                };

                _ = try io.msgRing(
                    &self.worker.io,
                    target_task,
                    .{
                        .ptr = self,
                        .msg = @intFromEnum(Msg.msg_ring_fail),
                        .cb = handleMsg,
                    },
                );
            }

            // Message each worker ring to shut down
            for (self.workers) |*worker| {
                const target_task = try io.getTask();
                target_task.* = .{
                    .userdata = worker,
                    .msg = @intFromEnum(Worker.Msg.shutdown),
                    .callback = Worker.handleMsg,
                    .req = .usermsg,
                };

                _ = try io.msgRing(
                    &worker.io,
                    target_task,
                    .{
                        .ptr = self,
                        .msg = @intFromEnum(Msg.msg_ring_fail),
                        .cb = handleMsg,
                    },
                );
            }
        },

        .shutdown_timeout => {
            self.shutdown_timer = null;
            if (self.poll_task) |pt| {
                _ = try pt.cancel(io, .{});
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
                    _ = try pt.cancel(io, .{});
                    self.poll_task = null;
                }
                if (self.shutdown_timer) |timer| {
                    _ = try timer.cancel(io, .{});
                    self.shutdown_timer = null;
                }
            }
        },
    }

    // We reap and submit our main worker ring here. It's possible that in this function our eventfd
    // won't wake up when we send a msg_ring message. Both of these calls are nonblocking, and we
    // only are in this function if we are shutting down, so we don't care too much about perf
    try self.worker.io.backend.reapCompletions(&self.worker.io);
    try self.worker.io.backend.submit(&self.worker.io.submission_q);
}

pub fn listenAndServe(self: *Server, io: *ourio.Ring, handler: hz.Handler) !void {
    self.io = io;

    {
        self.worker = try .init(self.gpa, self.timeout, handler, self.addr, self);
        self.worker.io = try io.initChild(64);
        try posix.listen(self.worker.fd, 64);
        self.worker.accept_task = try self.worker.io.accept(
            self.worker.fd,
            &self.worker.new_conn_addr.any,
            &self.worker.addr_size,
            .{
                .ptr = &self.worker,
                .msg = @intFromEnum(Worker.Msg.new_connection),
                .cb = Worker.handleMsg,
            },
        );

        var sock_len = self.addr.getOsSockLen();
        try posix.getsockname(self.worker.fd, &self.addr.any, &sock_len);

        try self.worker.io.backend.submit(&self.worker.io.submission_q);
    }

    for (self.workers, 0..) |*worker, i| {
        worker.* = try .init(self.gpa, self.timeout, handler, self.addr, self);

        self.threads[i] = try std.Thread.spawn(
            .{},
            Worker.run,
            .{ worker, io },
        );
    }
    // The main server ring needs a callback from the main ioring
    self.poll_task = try io.poll(try self.worker.io.backend.pollableFd(), posix.POLL.IN, .{
        .ptr = self,
        .msg = @intFromEnum(Server.Msg.main_worker_ready),
        .cb = Server.handleMsg,
    });
    _ = try io.poll(self.stop_pipe[0], posix.POLL.IN, .{
        .ptr = self,
        .msg = @intFromEnum(Server.Msg.shutdown),
        .cb = Server.handleMsg,
    });
}

pub fn deinit(self: *Server, gpa: Allocator) void {
    self.worker.deinit();
    for (self.workers) |*worker| {
        worker.deinit();
    }

    if (self.poll_task) |pt| {
        _ = pt.cancel(self.io, .{}) catch {};
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
    io: ourio.Ring,
    conn_pool: MemoryPool(Connection),
    state: Worker.State,
    timeout: Timeouts,
    handler: hz.Handler,

    shutting_down: bool = false,

    fd: posix.socket_t,
    accept_task: *ourio.Task,
    server: *Server,
    new_conn_addr: net.Address,
    addr_size: posix.socklen_t,

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
            .io = undefined,
            .state = .running,
            .conn_pool = .empty,
            .timeout = timeout,
            .handler = handler,
            .fd = fd,
            .keep_alive = 0,
            .accept_task = undefined,
            .server = server,
            .new_conn_addr = undefined,
            .addr_size = posix.sockaddr.SS_MAXSIZE,
        };
    }

    fn run(self: *Worker, main: *ourio.Ring) !void {
        self.io = try main.initChild(64);
        try posix.listen(self.fd, 64);
        self.accept_task = try self.io.accept(self.fd, &self.new_conn_addr.any, &self.addr_size, .{
            .ptr = self,
            .msg = @intFromEnum(Worker.Msg.new_connection),
            .cb = Worker.handleMsg,
        });

        try self.io.run(.until_done);
    }

    fn deinit(self: *Worker) void {
        self.conn_pool.deinit(self.gpa);
        self.io.deinit();
    }

    fn handleMsg(io: *ourio.Ring, task: ourio.Task) anyerror!void {
        const self = task.userdataCast(Worker);
        const result = task.result.?;
        switch (task.msgToEnum(Worker.Msg)) {
            .new_connection => {
                const fd = result.accept catch |err| {
                    switch (err) {
                        error.Canceled => {},
                        else => log.err("accept error: {}", .{err}),
                    }
                    return;
                };
                // We have to reset the addr_size before calling accept again
                self.addr_size = posix.sockaddr.SS_MAXSIZE;
                self.accept_task = try self.io.accept(
                    self.fd,
                    &self.new_conn_addr.any,
                    &self.addr_size,
                    .{
                        .ptr = self,
                        .msg = @intFromEnum(Worker.Msg.new_connection),
                        .cb = Worker.handleMsg,
                    },
                );
                const conn = try self.conn_pool.create(self.gpa);

                try conn.init(self.gpa, self, fd);
            },

            .shutdown => {
                self.state = .shutting_down;
                _ = try self.accept_task.cancel(io, .{});
                try self.maybeClose();
            },
        }
    }

    fn maybeClose(self: *Worker) !void {
        if (self.state != .shutting_down or self.keep_alive > 0) return;
        self.state = .done;
        if (!self.io.backend.done()) {
            // TODO: track only our tasks in a doubly linked list and only cancel our tasks
            try self.io.cancelAll();
        }

        const target_task = try self.io.getTask();
        target_task.* = .{
            .userdata = self.server,
            .msg = @intFromEnum(Server.Msg.worker_shutdown),
            .callback = Server.handleMsg,
            .req = .usermsg,
        };

        _ = try self.io.msgRing(self.server.io, target_task, .{});
    }
};

pub const Connection = struct {
    arena: std.heap.ArenaAllocator,
    worker: *Worker,

    buf: [4096]u8 = undefined,
    fd: posix.socket_t,
    write_buf: std.ArrayListUnmanaged(u8) = .empty,
    vecs: [2]posix.iovec_const = undefined,
    written: usize = 0,

    deadline: i64,

    ctx: hz.Context,

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
            .deadline = std.time.timestamp() + worker.timeout.read_header,
        };

        self.ctx = .{
            .arena = self.arena.allocator(),
            .deadline = 0,
            .io = &worker.io,
            .request = .{},
            .response = .{ .arena = self.arena.allocator() },
        };

        const task = try worker.io.recv(
            fd,
            &self.buf,
            .{
                .ptr = self,
                .msg = @intFromEnum(Connection.Msg.reading_headers),
                .cb = Connection.handleMsg,
            },
        );
        if (worker.timeout.read_header > 0) {
            try task.setDeadline(&worker.io, .{ .sec = self.deadline });
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
    };

    fn handleMsg(io: *ourio.Ring, task: ourio.Task) anyerror!void {
        const self = task.userdataCast(Connection);
        const result = task.result.?;
        state: switch (task.msgToEnum(Connection.Msg)) {
            .reading_headers => {
                assert(result == .recv);

                const n = result.recv catch continue :state .close;
                if (n == 0) continue :state .close;

                try self.ctx.request.appendSlice(self.arena.allocator(), self.buf[0..n]);

                switch (self.ctx.request.receivedHeader()) {
                    // When we receive the full header, we pass it to the handler
                    true => {
                        // Keep the worker alive since we are now handling this request
                        self.worker.keep_alive += 1;

                        // validate the request
                        if (try self.ctx.request.isValid(&self.ctx.response)) {
                            // Call the handler
                            try self.worker.handler.serveHttp(&self.ctx);
                        }
                    },

                    false => {
                        // We haven't received a full HEAD. prep another recv
                        const new_task = try io.recv(self.fd, &self.buf, .{
                            .ptr = self,
                            .msg = @intFromEnum(Connection.Msg.reading_headers),
                            .cb = Connection.handleMsg,
                        });
                        if (self.worker.timeout.read_header > 0) {
                            try new_task.setDeadline(io, .{ .sec = self.deadline });
                        }
                    },
                }
            },

            .reading_body => {
                assert(result == .recv);

                const n = result.recv catch continue :state .close;
                if (n == 0) continue :state .close;

                try self.ctx.request.appendSlice(self.arena.allocator(), self.buf[0..n]);

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
                if (self.worker.state == .shutting_down or !self.ctx.request.keepAlive()) {
                    continue :state .close;
                }

                // Keep the connection alive
                self.reset();
                const new_task = try io.recv(self.fd, &self.buf, .{
                    .ptr = self,
                    .msg = @intFromEnum(Connection.Msg.reading_headers),
                    .cb = Connection.handleMsg,
                });
                if (self.worker.timeout.idle > 0) {
                    self.deadline = std.time.timestamp() + self.worker.timeout.idle;
                    try new_task.setDeadline(io, .{ .sec = self.deadline });
                }
            },

            .close => {
                _ = try io.close(self.fd, .{
                    .ptr = self,
                    .msg = @intFromEnum(Connection.Msg.destroy),
                    .cb = Connection.handleMsg,
                });
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
        const resp = self.ctx.response;
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
        const head_len = self.ctx.request.headLen() orelse @panic("TODO");
        const cl = self.ctx.request.contentLength() orelse @panic("TODO");

        if (head_len + cl == self.ctx.request.bytes.items.len) {
            return self.worker.handler.serveHttp(&self.ctx);
        }

        self.state = .reading_body;

        _ = try self.worker.io.recv(self.fd, &self.buf, .{
            .ptr = self,
            .msg = @intFromEnum(Connection.Msg.reading_body),
            .cb = Connection.handleMsg,
        });
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
        const body = switch (self.ctx.response.body) {
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
            .headers_only => try self.worker.io.write(self.fd, headers[self.written..], .beginning, .{
                .ptr = self,
                .msg = @intFromEnum(Connection.Msg.write_response),
                .cb = Connection.handleMsg,
            }),

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

                break :blk try self.worker.io.writev(self.fd, &self.vecs, .beginning, .{
                    .ptr = self,
                    .msg = @intFromEnum(Connection.Msg.write_response),
                    .cb = Connection.handleMsg,
                });
            },

            .body_only => blk: {
                const offset = self.written - headers.len;
                const unwritten_body = body[offset..];
                break :blk try self.worker.io.write(self.fd, unwritten_body, .beginning, .{
                    .ptr = self,
                    .msg = @intFromEnum(Connection.Msg.write_response),
                    .cb = Connection.handleMsg,
                });
            },
        };

        if (self.worker.timeout.write > 0) {
            try new_task.setDeadline(&self.worker.io, .{ .sec = self.deadline });
        }
    }

    fn reset(self: *Connection) void {
        _ = self.arena.reset(.free_all);
        self.ctx = .{
            .arena = self.arena.allocator(),
            .io = &self.worker.io,
            .response = .{ .arena = self.arena.allocator() },
        };

        self.write_buf = .empty;
        self.written = 0;
    }

    fn responseComplete(self: *Connection) bool {
        return self.written == (self.write_buf.items.len + self.ctx.response.body.len());
    }
};

test "server" {
    const gpa = std.testing.allocator;
    var ring = try ourio.Ring.init(gpa, 8);
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
            ctx: *hz.Context,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx.userdata));
            self.got_request = true;
            try ctx.response.any().print("hello world", .{});
            try ctx.response.flush();
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
