const std = @import("std");
const io = @import("io");
const sniff = @import("sniff.zig");

const log = std.log.scoped(.horizon);

const client = @import("client.zig");
const http = std.http;
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const MemoryPool = @import("pool.zig").MemoryPool;

const assert = std.debug.assert;

const @"Content-Length" = "Content-Length";
const @"Content-Type" = "Content-Type";
const crlf = "\r\n";

pub const Handler = struct {
    ptr: *anyopaque,
    serveFn: *const fn (*anyopaque, *Context, ResponseWriter, Request) anyerror!void,

    /// Initialize a handler from a Type which has a serveHttp method
    pub fn init(comptime T: type, ptr: *T) Handler {
        return .{ .ptr = ptr, .serveFn = T.serveHttp };
    }

    pub fn serveHttp(self: Handler, ctx: *Context, w: ResponseWriter, r: Request) anyerror!void {
        return self.serveFn(self.ptr, ctx, w, r);
    }
};

pub const Worker = struct {
    should_quit: bool,
    ring: io.Ring,
    handler: Handler,
    conn_pool: MemoryPool(Connection),

    /// Number of active completions we have on the queue that we want to wait for before gracefully
    /// shutting down
    keep_alive: u8,

    timeout: Server.Timeouts,

    const State = enum {
        init,
        running,
        finishing,
        canceling,
        done,
    };

    fn serve(self: *Worker, gpa: Allocator, server: *Server) !void {
        defer self.conn_pool.deinit(gpa);

        state: switch (State.init) {
            .init => {
                self.ring = try server.ring.initChild();
                continue :state .running;
            },

            .running => {
                while (!self.should_quit) {
                    try self.ring.submitAndWait();

                    while (self.ring.nextCompletion()) |cqe| {
                        try self.handleCqe(gpa, cqe);
                    }
                }

                continue :state .finishing;
            },

            .finishing => {
                while (self.keep_alive > 0) {
                    try self.ring.submitAndWait();

                    while (self.ring.nextCompletion()) |cqe| {
                        try self.handleCqe(gpa, cqe);
                    }
                }

                continue :state .canceling;
            },

            .canceling => {
                try self.ring.cancelAll();

                // We could get cancelations before we know how many to expect, so we track both the
                // number of completions and how many we expect
                var total: usize = 1;
                var canceled: usize = 0;
                while (canceled < total) {
                    try self.ring.submitAndWait();
                    while (self.ring.nextCompletion()) |cqe| {
                        canceled += 1;
                        if (cqe.userdata == 0) {
                            // This is our cancellation cqe
                            const res = cqe.unwrap() catch |err| {
                                log.err("cancel all error: {}", .{err});
                                continue;
                            };
                            total += res;
                            continue;
                        }
                        try self.handleCqe(gpa, cqe);
                    }
                }

                continue :state .done;
            },

            .done => {
                // Notify the main thread we have exited
                try self.ring.msgRing(server.ring, 0, @intFromEnum(Server.Op.thread_exit), 0);
                try self.ring.submit();
            },
        }
    }

    fn handleCqe(
        self: *Worker,
        gpa: Allocator,
        cqe: io.Completion,
    ) !void {
        const c: *Event = @ptrFromInt(cqe.userdata);
        switch (c.parent) {
            .server => switch (c.op) {
                .accept => {
                    // This accept was sent from the Server, and we handled errors there
                    const result = cqe.unwrap() catch unreachable;

                    const conn = try self.conn_pool.create(gpa);
                    try conn.init(gpa, self, result);
                },

                .stop => self.should_quit = true,
                else => unreachable,
            },

            .connection => {
                switch (c.op) {
                    .recv, .send => {
                        const conn: *Connection = @alignCast(@fieldParentPtr("op_c", c));
                        try conn.onEvent(self, cqe, c);
                    },

                    .close => {
                        const conn: *Connection = @alignCast(@fieldParentPtr("op_c", c));
                        conn.deinit();
                        self.conn_pool.destroy(conn);
                    },

                    .timeout => {},

                    else => unreachable,
                }
            },

            .client_connection => {
                log.debug("cqe", .{});
                switch (c.op) {
                    .socket, .connect, .recv => {
                        const conn: *client.Connection = @alignCast(@fieldParentPtr("op_c", c));
                        try conn.onEvent(self, cqe, c);
                    },
                    .send => {
                        const conn: *client.Connection = @alignCast(@fieldParentPtr("send_c", c));
                        try conn.onEvent(self, cqe, c);
                    },
                    else => @panic("todo"),
                }
            },
        }
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
    accept_c: Event = .{ .parent = .server, .op = .accept },

    timeout: Timeouts,

    shutdown_timeout: u8,

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
            .timeout = .{
                .read_header = opts.read_header_timeout,
                .read_body = opts.read_body_timeout,
                .idle = opts.idle_timeout,
                .write = opts.write_timeout,
            },
            .shutdown_timeout = opts.shutdown_timeout,
        };

        try posix.bind(fd, &self.addr.any, addr.getOsSockLen());
        try posix.listen(fd, 64);

        var sock_len = addr.getOsSockLen();
        try posix.getsockname(fd, &self.addr.any, &sock_len);
    }

    pub fn deinit(self: *Server, gpa: Allocator) void {
        for (self.workers) |*worker| {
            worker.ring.deinit();
        }
        self.ring.deinit();

        gpa.free(self.workers);
        gpa.free(self.threads);
    }

    pub fn serve(self: *Server, gpa: Allocator, handler: Handler) !void {
        // If we have a shutdown_signal, we have to register it before spawning threads. Each thread
        // inherits the signal mask
        var sfd: ?posix.fd_t = null;
        defer if (sfd) |fd| posix.close(fd);

        // The completion pointer we'll send to to workers at shutdown
        var stop_c: Event = .{ .parent = .server, .op = .stop };

        const State = enum {
            init,
            running,
            stopping,
            shutting_down,
        };

        var spawned_threads: usize = 0;
        errdefer {
            for (0..spawned_threads) |i| {
                self.threads[i].detach();
            }
        }

        state: switch (State.init) {
            .init => {
                if (self.shutdown_signal > 0) {
                    // Listen for the shutdown signal
                    sfd = try self.ring.signalfd(self.shutdown_signal, @intFromEnum(Op.stop));
                }

                for (self.workers, 0..) |*worker, i| {
                    worker.* = .{
                        .should_quit = false,
                        .ring = undefined,
                        .handler = handler,
                        .keep_alive = 0,
                        .timeout = self.timeout,
                        .conn_pool = .empty,
                    };
                    worker.handler = handler;
                    self.threads[i] = try std.Thread.spawn(.{}, Worker.serve, .{ worker, gpa, self });
                    spawned_threads += 1;
                }

                // Start listening for stop signals from the stop pipe
                try self.ring.poll(self.stop_pipe[0], @intFromEnum(Op.stop));

                // Start accepting connections
                try self.ring.accept(self.fd, @intFromEnum(Op.accept));

                continue :state .running;
            },

            .running => {
                try self.ring.submitAndWait();

                while (self.ring.nextCompletion()) |cqe| {
                    const op: Op = @enumFromInt(cqe.userdata);
                    switch (op) {
                        .accept => try self.acceptAndSend(cqe),

                        .msg_ring => _ = cqe.unwrap() catch |err|
                            log.err("msg not sent to ring: {}", .{err}),

                        // When we receive the stop signal, we transition to stopping state
                        .stop => continue :state .stopping,

                        // When we receive the cancel accept response, we exit the loop
                        .cancel_accept,
                        .thread_exit,
                        .timeout,
                        => unreachable,
                    }
                }

                continue :state .running;
            },

            .stopping => {
                if (sfd) |fd| {
                    var buf: [@sizeOf(posix.siginfo_t)]u8 = undefined;
                    _ = posix.read(fd, &buf) catch {};
                }

                // We start a few events to close:
                // 1. A timer that will close use no matter what when it expires
                // 2. A cancelation of the accept submission
                // 3. A message to each worker to finish up working. The workers will message the
                //    main thread when they exit

                // 60 second timeout to gracefully close
                try self.ring.timer(self.shutdown_timeout, @intFromEnum(Op.timeout));

                try self.ring.cancel(
                    @intFromEnum(Op.accept),
                    @intFromEnum(Op.cancel_accept),
                );

                for (self.workers) |worker| {
                    try self.ring.msgRing(
                        worker.ring,
                        0,
                        @intFromPtr(&stop_c),
                        @intFromEnum(Op.msg_ring),
                    );
                }

                continue :state .shutting_down;
            },

            .shutting_down => {
                var exited_threads: usize = 0;

                while (exited_threads < self.workers.len) {
                    try self.ring.submitAndWait();

                    while (self.ring.nextCompletion()) |cqe| {
                        const op: Op = @enumFromInt(cqe.userdata);
                        switch (op) {
                            // it's possible we accepted a connection before the cancelation. Close
                            // the fd if we did. The CQE will have an error when accept was
                            // canceled, so we continue on error
                            .accept => {
                                const fd = cqe.unwrap() catch continue;
                                posix.close(fd);
                            },

                            // We'll get a msg_ring event if we failed to send the stop message to
                            // the workers
                            .msg_ring => {
                                _ = cqe.unwrap() catch |err|
                                    log.err("msg not sent to ring: {}", .{err});
                            },

                            // If we receive another stop signal, we return immediately with an
                            // error
                            .stop => {
                                if (sfd) |fd| {
                                    var buf: [@sizeOf(posix.siginfo_t)]u8 = undefined;
                                    _ = posix.read(fd, &buf) catch {};
                                }
                                return error.ForceStop;
                            },

                            // We get this event if the accept failed to cancel. There's nothing we
                            // can really do if it fails though, we are already closing connections
                            // that come in
                            .cancel_accept => {},

                            // When a thread exits, it sends us this message
                            .thread_exit => exited_threads += 1,

                            // On timeout, we just return immediately with an error
                            .timeout => return error.Timeout,
                        }
                    }
                }

                // If we've exited the loop, it's because we closed all the threads cleanly
                for (self.threads) |thread| {
                    thread.join();
                }
            },
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
pub const Event = struct {
    parent: enum {
        server,
        connection,
        client_connection,
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
        timeout,
        socket,
        connect,
    },
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

    op_c: Event = .{ .parent = .connection, .op = .recv },
    timeout_c: Event = .{ .parent = .connection, .op = .timeout },

    /// Write buffer
    write_buf: std.ArrayListUnmanaged(u8) = .empty,

    /// An active request
    request: Request = .{},

    response: Response,

    ctx: Context,

    vecs: [2]posix.iovec_const,
    written: usize = 0,

    /// deadline to perform the next action
    deadline: i64,

    state: State = .init,

    const State = enum {
        init,
        reading_headers,
        handling_request,
        send,
        send_with_deadline,
        waiting_send_response,
        idle,
        close,
        destroy,
        waiting_for_destruction,
    };

    const WriteState = enum {
        headers_only,
        headers_and_body,
        body_only,
    };

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
            .ctx = undefined,
            .vecs = undefined,
            .deadline = std.time.timestamp() + worker.timeout.read_header,
        };

        self.response = .{ .arena = self.arena.allocator() };
        self.ctx = .{
            .arena = self.arena.allocator(),
            .deadline = 0,
            .userdata = .empty,
        };

        if (worker.timeout.read_header > 0) {
            try worker.ring.recvWithDeadline(
                self.fd,
                &self.buf,
                self.deadline,
                &self.op_c,
                &self.timeout_c,
            );
        } else {
            try worker.ring.recv(self.fd, &self.buf, &self.op_c);
        }
    }

    pub fn deinit(self: *Connection) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn onEvent(
        self: *Connection,
        worker: *Worker,
        cqe: io.Completion,
        event: *Event,
    ) !void {
        state: switch (self.state) {
            // Initial state. We are here at first connection, or coming out of idle
            .init => {
                assert(event.op == .recv);

                continue :state .reading_headers;
            },

            // We are consuming bytes until we have read the full header section
            .reading_headers => {
                assert(event.op == .recv);

                self.state = .reading_headers;

                const result = cqe.unwrap() catch continue :state .close;
                if (result == 0) continue :state .close;

                try self.request.appendSlice(self.arena.allocator(), self.buf[0..result]);

                switch (self.request.receivedHeader()) {
                    // When we receive the full header, we pass it to the handler
                    true => continue :state .handling_request,

                    false => {
                        self.op_c.op = .recv;
                        // We haven't received a full HEAD. prep another recv
                        if (worker.timeout.read_header > 0) {
                            try worker.ring.recvWithDeadline(
                                self.fd,
                                &self.buf,
                                self.deadline,
                                &self.op_c,
                                &self.timeout_c,
                            );
                        } else {
                            try worker.ring.recv(self.fd, &self.buf, &self.op_c);
                        }
                    },
                }
            },

            .handling_request => {
                self.state = .handling_request;

                // Keep the worker alive since we are now handling this request
                worker.keep_alive += 1;

                const tls_client = try client.Client.init(self.arena.allocator());
                const conn = try self.arena.allocator().create(client.Connection);
                try conn.init(self.arena.allocator(), tls_client.cert, worker, "timculverhouse.com", 443);

                return;
                // // validate the request
                // if (try self.request.isValid(self.response.responseWriter())) {
                //     // Call the handler
                //     try worker.handler.serveHttp(
                //         &self.ctx,
                //         self.response.responseWriter(),
                //         self.request,
                //     );
                //
                //     // TODO: we need to add some future handling for IO ops here. We want to give the
                //     // handler an option to do IO async in our event loop. IE they could call API
                //     // endpoints and return without wanting to write the response
                // }
                //
                // // Prepare the header
                // try self.prepareHeader();
                //
                // switch (worker.timeout.write) {
                //     0 => continue :state .send,
                //     else => {
                //         self.deadline = std.time.timestamp() + worker.timeout.write;
                //         continue :state .send_with_deadline;
                //     },
                // }
            },

            .send => {
                self.state = .waiting_send_response;
                self.op_c.op = .send;

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

                switch (wstate) {
                    .headers_only => {
                        try worker.ring.write(self.fd, headers[self.written..], &self.op_c);
                    },

                    .headers_and_body => {
                        const unwritten = headers[self.written..];
                        self.vecs[0] = .{
                            .base = unwritten.ptr,
                            .len = unwritten.len,
                        };
                        self.vecs[1] = .{
                            .base = body.ptr,
                            .len = body.len,
                        };
                        try worker.ring.writev(self.fd, &self.vecs, &self.op_c);
                    },

                    .body_only => {
                        const offset = self.written - headers.len;
                        const unwritten_body = body[offset..];
                        try worker.ring.write(self.fd, unwritten_body, &self.op_c);
                    },
                }
            },

            .send_with_deadline => {
                self.state = .waiting_send_response;
                self.op_c.op = .send;

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

                switch (wstate) {
                    .headers_only => {
                        try worker.ring.writeWithDeadline(
                            self.fd,
                            headers[self.written..],
                            self.deadline,
                            &self.op_c,
                            &self.timeout_c,
                        );
                    },

                    .headers_and_body => {
                        const unwritten = headers[self.written..];
                        self.vecs[0] = .{
                            .base = unwritten.ptr,
                            .len = unwritten.len,
                        };
                        self.vecs[1] = .{
                            .base = body.ptr,
                            .len = body.len,
                        };
                        try worker.ring.writevWithDeadline(
                            self.fd,
                            &self.vecs,
                            self.deadline,
                            &self.op_c,
                            &self.timeout_c,
                        );
                    },

                    .body_only => {
                        const offset = self.written - headers.len;
                        const unwritten_body = body[offset..];
                        try worker.ring.writeWithDeadline(
                            self.fd,
                            unwritten_body,
                            self.deadline,
                            &self.op_c,
                            &self.timeout_c,
                        );
                    },
                }
            },

            .waiting_send_response => {
                assert(event.op == .send);

                const result = cqe.unwrap() catch {
                    // On error we are done and can let the worker softclose if it wants
                    worker.keep_alive -= 1;
                    continue :state .close;
                };

                self.written += result;

                if (!self.responseComplete()) {
                    if (worker.timeout.write > 0)
                        continue :state .send_with_deadline
                    else
                        continue :state .send;
                }

                // Response sent. Decide if we should close the connection or keep it alive
                // We also could allow the worker to exit gracefully now
                worker.keep_alive -= 1;

                // If the worker is quitting, we can close this connection
                if (worker.should_quit or !self.request.keepAlive()) {
                    continue :state .close;
                }

                continue :state .idle;
            },

            .idle => {
                // We'll go back to init state after reinitializing state
                defer self.state = .init;

                self.reset();

                // prep another recv
                if (worker.timeout.idle > 0) {
                    self.deadline = std.time.timestamp() + worker.timeout.idle;
                    try worker.ring.recvWithDeadline(
                        self.fd,
                        &self.buf,
                        self.deadline,
                        &self.op_c,
                        &self.timeout_c,
                    );
                } else {
                    try worker.ring.recv(self.fd, &self.buf, &self.op_c);
                }
            },

            .close => {
                // If the worker is closing, we don't schedule new SQEs and instead close
                // synchronously
                if (worker.should_quit) continue :state .destroy;

                self.state = .waiting_for_destruction;
                self.op_c.op = .close;
                try worker.ring.close(self.fd, &self.op_c);
            },

            .waiting_for_destruction => {},

            .destroy => {
                assert(worker.should_quit);

                self.deinit();
                worker.conn_pool.destroy(self);
            },
        }
    }

    fn reset(self: *Connection) void {
        _ = self.arena.reset(.retain_capacity);
        self.request = .{};
        self.write_buf = .empty;
        self.response = .{ .arena = self.arena.allocator() };
        self.written = 0;
        self.op_c.op = .recv;

        self.ctx.deadline = 0;
        self.ctx.userdata = .empty;
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

        if (resp.headers.get(@"Content-Length") == null) {
            writer.print(
                @"Content-Length" ++ ": {d}" ++ crlf,
                .{resp.body.len()},
            ) catch unreachable;
        }

        if (resp.headers.get(@"Content-Type") == null) {
            const ct = switch (resp.body) {
                .file => unreachable,
                .static => |s| sniff.detectContentType(s),
                .dynamic => |*d| sniff.detectContentType(d.items),
            };
            writer.print(
                @"Content-Type" ++ ": {s}" ++ crlf,
                .{ct},
            ) catch unreachable;
        }

        var iter = resp.headers.iterator();
        while (iter.next()) |h| {
            writer.print(
                "{s}: {s}" ++ crlf,
                .{ h.key_ptr.*, h.value_ptr.* },
            ) catch unreachable;
        }

        writer.writeAll(crlf) catch unreachable;
    }

    fn responseComplete(self: *Connection) bool {
        return self.written == (self.write_buf.items.len + self.response.body.len());
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
            crlf ++ crlf,
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
            if (std.ascii.eqlIgnoreCase(header.name, key)) return header.value;
        }
        return null;
    }

    /// Returns the HTTP method of this request
    pub fn method(self: Request) http.Method {
        assert(self.receivedHeader());
        // GET / HTTP/1.1
        const idx = std.mem.indexOf(u8, self.bytes.items, crlf) orelse unreachable;
        const line = self.bytes.items[0..idx];
        const space = std.mem.indexOfScalar(u8, line, ' ') orelse @panic("TODO: bad request");
        const val = http.Method.parse(line[0..space]);
        return @enumFromInt(val);
    }

    pub fn contentLength(self: Request) ?u64 {
        const value = self.getHeader(@"Content-Length") orelse return null;
        return std.fmt.parseUnsigned(u64, value, 10) catch @panic("TODO: bad content length");
    }

    pub fn host(self: Request) []const u8 {
        return self.getHeader("Host") orelse @panic("TODO: no host header");
    }

    pub fn keepAlive(self: Request) bool {
        const value = self.getHeader("Connection") orelse return true;
        // fast and slow paths for case matching
        return std.ascii.eqlIgnoreCase(value, "keep-alive");
    }

    pub fn path(self: Request) []const u8 {
        assert(self.receivedHeader());
        const idx = std.mem.indexOf(u8, self.bytes.items, crlf) orelse unreachable;
        const line = self.bytes.items[0..idx];
        var iter = std.mem.splitScalar(u8, line, ' ');
        _ = iter.first();
        return iter.next() orelse unreachable;
    }

    /// Validates a request
    fn isValid(self: Request, w: ResponseWriter) !bool {
        const m = self.method();
        if (m.requestHasBody()) {
            // We require a content length
            if (self.contentLength() == null) {
                try errorResponse(w, .bad_request, @"Content-Length" ++ " is required", .{});
                return false;
            }
        }
        return true;
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

    body: Body = .{ .dynamic = .empty },

    headers: std.StringHashMapUnmanaged([]const u8) = .{},

    status: ?http.Status = null,

    const VTable: ResponseWriter.VTable = .{
        .setHeader = Response.setHeader,
        .setStatus = Response.setStatus,
        .write = Response.typeErasedWrite,
    };

    const Body = union(enum) {
        file: struct {
            fd: posix.fd_t,
            size: usize,
        },
        static: []const u8,
        dynamic: std.ArrayListUnmanaged(u8),

        fn len(self: Body) usize {
            return switch (self) {
                .file => |f| f.size,
                .static => |s| s.len,
                .dynamic => |d| d.items.len,
            };
        }
    };

    fn responseWriter(self: *Response) ResponseWriter {
        return .{ .ptr = self, .vtable = &VTable };
    }

    fn setHeader(ptr: *anyopaque, k: []const u8, maybe_v: ?[]const u8) Allocator.Error!void {
        const self: *Response = @ptrCast(@alignCast(ptr));
        if (maybe_v) |v| {
            return self.headers.put(self.arena, k, v);
        }

        _ = self.headers.remove(k);
    }

    fn setStatus(ptr: *anyopaque, status: http.Status) void {
        const self: *Response = @ptrCast(@alignCast(ptr));
        self.status = status;
    }

    fn typeErasedWrite(ptr: *const anyopaque, bytes: []const u8) anyerror!usize {
        const self: *Response = @constCast(@ptrCast(@alignCast(ptr)));
        switch (self.body) {
            .file => |file| {
                // If we had set a file, we close the descriptor here
                posix.close(file.fd);
                self.body = .{ .dynamic = .empty };
            },
            .static => self.body = .{ .dynamic = .empty },
            .dynamic => {},
        }
        try self.body.dynamic.appendSlice(self.arena, bytes);
        return bytes.len;
    }
};

pub const HeaderIterator = struct {
    iter: std.mem.SplitIterator(u8, .sequence),

    pub fn init(bytes: []const u8) HeaderIterator {
        var iter = std.mem.splitSequence(u8, bytes, "\r\n");
        // Throw away the first line
        _ = iter.first();
        return .{ .iter = iter };
    }

    pub fn next(self: *HeaderIterator) ?std.http.Header {
        const line = self.iter.next() orelse return null;
        if (line.len == 0) {
            // When we get to the first empty line we are done
            self.iter.index = self.iter.buffer.len;
            return null;
        }
        var kv_iter = std.mem.splitScalar(u8, line, ':');
        const name = kv_iter.first();
        const value = kv_iter.rest();
        return .{
            .name = name,
            .value = std.mem.trim(u8, value, " \t"),
        };
    }
};

pub const Context = struct {
    /// Arena allocator which will be freed once the response has been written
    arena: Allocator,

    /// Deadline for the response, in seconds from unix epoch
    deadline: u64,

    /// Userdata that will live as long as the request. Always use the arena allocator when adding
    /// data to the hashmap. Keys must be stable for the lifetime of the request - they can be duped
    /// with the arena if needed
    userdata: std.StringHashMapUnmanaged(u64),

    pub fn expired(self: Context) bool {
        if (self.deadline == 0) return false;
        return std.time.timestamp() > self.deadline;
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
    try w.setHeader(@"Content-Length", null);
    // Set content type
    try w.setHeader(@"Content-Type", "text/plain");
    // Print the body
    try w.any().print(format, args);
}

/// Serve a static buffer. The buffer will not be sent through middleware - if using gzip middleware
/// or something else which modifies the Content-Encoding, this must be handled first. The buffer is
/// what will exactly be sent to the client
pub fn serveStaticBuffer(ctx: *Context, buffer: []const u8) !void {
    const conn: *Connection = @alignCast(@fieldParentPtr("ctx", ctx));
    conn.response.body = .{ .static = buffer };
}

test Server {
    const addr: net.Address = try .parseIp4("127.0.0.1", 0);
    const opts: Server.Options = .{ .addr = addr, .workers = 1, .shutdown_timeout = 1 };
    var server: Server = undefined;

    const TestHandler = struct {
        got_request: bool = false,

        fn handler(self: *@This()) Handler {
            return .init(@This(), self);
        }

        pub fn serveHttp(ptr: *anyopaque, _: *Context, w: ResponseWriter, _: Request) anyerror!void {
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

    {
        _ = try stream.write("GET / HTTP/1.1\r\nConnection: keep-alive\r\n\r\n");
        var buf: [4096]u8 = undefined;
        const n = try stream.read(&buf);

        try std.testing.expect(handler.got_request);
        try std.testing.expectStringStartsWith(buf[0..n], "HTTP/1.1 200");
    }

    {
        handler.got_request = false;
        _ = try stream.write("POST / HTTP/1.1\r\nConnection: keep-alive\r\n\r\n");
        var buf: [4096]u8 = undefined;
        const n = try stream.read(&buf);

        // We shouldn't get called since the request is invalid
        try std.testing.expect(!handler.got_request);
        try std.testing.expectStringStartsWith(buf[0..n], "HTTP/1.1 400");
    }

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

test {
    _ = @import("sniff.zig");
    _ = @import("pool.zig");
}
