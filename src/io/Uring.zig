const Uring = @This();

const std = @import("std");

const io = @import("io.zig");

const Allocator = std.mem.Allocator;
const Queue = @import("queue.zig").Intrusive;
const assert = std.debug.assert;
const linux = std.os.linux;
const posix = std.posix;

const common_flags: u32 =
    linux.IORING_SETUP_SUBMIT_ALL | // Keep submitting events even if one had an error
    linux.IORING_SETUP_CLAMP | // Clamp entries to system supported max
    linux.IORING_SETUP_DEFER_TASKRUN | // Defer work until we submit tasks. Requires SINGLE_ISSUER
    linux.IORING_SETUP_COOP_TASKRUN | // Don't interupt userspace when task is complete
    linux.IORING_SETUP_SINGLE_ISSUER; // Only a single thread will issue tasks

const msg_ring_received_cqe = 1 << 8;

gpa: Allocator,
ring: linux.IoUring,
free_list: Queue(io.Task, .free) = .{},
work_queue: Queue(io.Task, .in_flight) = .{},
inflight: usize = 0,
run_cond: io.RunCondition,

/// Initialize a Ring
pub fn init(gpa: Allocator, entries: u16) !Uring {
    var params = std.mem.zeroInit(linux.io_uring_params, .{
        .flags = common_flags,
        .sq_thread_idle = 1000,
    });

    return .{ .gpa = gpa, .ring = try .init_params(entries, &params), .run_cond = .once };
}

pub fn deinit(self: *Uring) void {
    while (self.free_list.pop()) |task| self.gpa.destroy(task);
    while (self.work_queue.pop()) |task| self.gpa.destroy(task);

    if (self.ring.fd < 0) return;
    self.ring.deinit();
    self.* = undefined;
}

/// Initializes a child Ring which can be woken up by self. This must be called from the thread
/// which will operate the child ring. Initializes with the same queue size as the parent
pub fn initChild(self: Uring, entries: u16) !Uring {
    const flags: u32 = common_flags | linux.IORING_SETUP_ATTACH_WQ;

    var params = std.mem.zeroInit(linux.io_uring_params, .{
        .flags = flags,
        .sq_thread_idle = 1000,
        .wq_fd = @as(u32, @bitCast(self.ring.fd)),
    });

    return .{
        .gpa = self.gpa,
        .ring = try .init_params(entries, &params),
        .run_cond = .once,
    };
}

pub fn run(self: *Uring, limit: io.RunCondition) !void {
    self.run_cond = limit;
    while (true) {
        try self.submit();
        try self.reapCompletions();
        switch (self.run_cond) {
            .once => return,
            .until_done => if (self.inflight == 0 and self.work_queue.empty()) return,
            .forever => {},
        }
    }
}

fn reapCompletions(self: *Uring) anyerror!void {
    var cqes: [64]linux.io_uring_cqe = undefined;
    const n = self.ring.copy_cqes(&cqes, 1) catch |err| {
        switch (err) {
            error.SignalInterrupt => return,
            else => return err,
        }
    };
    for (cqes[0..n]) |cqe| {
        const task: *io.Task = @ptrFromInt(cqe.user_data);

        const result: io.Result = switch (task.req) {
            .noop => .noop,

            // Deadlines we don't do anything for, these are always sent to a noopCallback
            .deadline => .{ .deadline = {} },

            .timer => .{ .timer = switch (cqeToE(cqe.res)) {
                .SUCCESS, .TIME => {},
                .INVAL, .FAULT => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .cancel => .{ .cancel = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                .NOENT => io.CancelError.EntryNotFound,
                .ALREADY => io.CancelError.NotCanceled,
                else => |e| unexpectedError(e),
            } },

            .accept => .{ .accept = switch (cqeToE(cqe.res)) {
                .SUCCESS => cqe.res,
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .msg_ring => .{ .msg_ring = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .recv => .{ .recv = switch (cqeToE(cqe.res)) {
                .SUCCESS => @intCast(cqe.res),
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                .CONNRESET => io.RecvError.ConnectionResetByPeer,
                else => |e| unexpectedError(e),
            } },

            .write => .{ .write = switch (cqeToE(cqe.res)) {
                .SUCCESS => @intCast(cqe.res),
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .writev => .{ .writev = switch (cqeToE(cqe.res)) {
                .SUCCESS => @intCast(cqe.res),
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .close => .{ .close = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .poll => .{ .poll = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .usermsg => .{ .usermsg = @intCast(cqe.res) },
        };

        if (cqe.flags & msg_ring_received_cqe != 0) {
            // This message was received from another ring. We don't decrement inflight for this.
            // But we do need to set the task as free because we will add it to our free list
            task.state = .free;
        } else if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
            // If the cqe doesn't have IORING_CQE_F_MORE set, then this task is complete and free to
            // be rescheduled
            self.inflight -= 1;
            task.state = .free;
        }

        try task.callback(self, task, result);

        // The callback could reschedule the task. So we handle it's state after the callback
        switch (task.state) {
            .free => {
                task.* = undefined;
                // Overcome a nice assertion
                task.next = null;
                task.state = .free;
                self.free_list.push(task);
            },

            .in_flight => {},
        }
    }
}

pub fn submitAndWait(self: *Uring) !void {
    var sqes_available = self.sqesAvailable();
    while (self.work_queue.pop()) |task| {
        const sqes_required = sqesRequired(task);
        if (sqes_available < sqes_required) {
            sqes_available += try self.ring.submit();
            continue;
        }
        defer sqes_available -= sqes_required;
        self.prepTask(task);
    }

    while (true) {
        _ = self.ring.submit_and_wait(1) catch |err| {
            switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            }
        };
        return;
    }
}

pub fn submit(self: *Uring) !void {
    var sqes_available = self.sqesAvailable();
    while (self.work_queue.pop()) |task| {
        const sqes_required = sqesRequired(task);
        if (sqes_available < sqes_required) {
            sqes_available += try self.ring.submit();
            continue;
        }
        defer sqes_available -= sqes_required;
        self.prepTask(task);
    }
    _ = try self.ring.submit();
}

fn sqesRequired(task: *const io.Task) u32 {
    return if (task.deadline == null) 1 else 2;
}

fn sqesAvailable(self: *Uring) u32 {
    return @intCast(self.ring.sq.sqes.len - self.ring.sq_ready());
}

pub fn prepTask(self: *Uring, task: *io.Task) void {
    switch (task.req) {
        .noop => {
            const sqe = self.getSqe();
            sqe.prep_nop();
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        // Deadlines are always prepared from their parent task
        .deadline => unreachable,

        .timer => |*t| {
            const sqe = self.getSqe();
            sqe.prep_timeout(@ptrCast(t), 0, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .cancel => |c| {
            const sqe = self.getSqe();
            switch (c) {
                .all => sqe.prep_cancel(0, linux.IORING_ASYNC_CANCEL_ANY),
                .task => |t| sqe.prep_cancel(@intFromPtr(t), 0),
            }
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .accept => |fd| {
            const sqe = self.getSqe();
            sqe.prep_multishot_accept(fd, null, null, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .msg_ring => |msg| {
            const sqe = self.getSqe();
            sqe.prep_rw(.MSG_RING, msg.target.ring.fd, 0, msg.result, @intFromPtr(msg.task));
            sqe.user_data = @intFromPtr(task);
            // Pass flags on the sent CQE. We use this to distinguish between a received message and
            // a message freom our own loop
            sqe.rw_flags |= linux.IORING_MSG_RING_FLAGS_PASS;
            sqe.splice_fd_in |= msg_ring_received_cqe;
            self.prepDeadline(task, sqe);
        },

        .recv => |req| {
            const sqe = self.getSqe();
            sqe.prep_recv(req.fd, req.buffer, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .write => |req| {
            const sqe = self.getSqe();
            sqe.prep_write(req.fd, req.buffer, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .writev => |req| {
            const sqe = self.getSqe();
            sqe.prep_writev(req.fd, req.vecs, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .close => |fd| {
            const sqe = self.getSqe();
            sqe.prep_close(fd);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .poll => |req| {
            const sqe = self.getSqe();
            sqe.prep_poll_add(req.fd, req.mask);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        // usermsg is only ever sent from one ring to another. There is nothing to prepare
        .usermsg => unreachable,
    }
}

fn prepDeadline(self: *Uring, parent_task: *io.Task, parent_sqe: *linux.io_uring_sqe) void {
    const task = parent_task.deadline orelse return;
    assert(task.req == .deadline);
    parent_sqe.flags |= linux.IOSQE_IO_LINK;

    const sqe = self.getSqe();
    const flags = linux.IORING_TIMEOUT_ABS | // absolute time
        linux.IORING_TIMEOUT_REALTIME; // use the realtime clock (as opposed to boot time)
    sqe.prep_link_timeout(@ptrCast(&task.req.deadline), flags);
    sqe.user_data = @intFromPtr(task);
}

pub fn nextCompletion(self: *Uring) ?linux.io_uring_cqe {
    const ready = self.ring.cq_ready();
    if (ready == 0) return null;
    const head = self.ring.cq.head.* & self.ring.cq.mask;
    const cqe = self.ring.cq.cqes[head];
    self.ring.cq_advance(1);

    return cqe;
}

/// Get an sqe from the ring. Caller should only call this function if they are sure we have an SQE
/// available. Asserts that we have one available
fn getSqe(self: *Uring) *linux.io_uring_sqe {
    assert(self.ring.sq.sqes.len > self.ring.sq_ready());
    self.inflight += 1;
    return self.ring.get_sqe() catch unreachable;
}

fn cqeToE(result: i32) std.posix.E {
    if (result > -4096 and result < 0) {
        return @as(std.posix.E, @enumFromInt(-result));
    }
    return .SUCCESS;
}

pub fn getTask(self: *Uring) Allocator.Error!*io.Task {
    return self.free_list.pop() orelse try self.gpa.create(io.Task);
}

pub fn noop(
    self: *Uring,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .noop,
    };

    self.work_queue.push(task);
    return task;
}

pub fn timer(
    self: *Uring,
    duration: io.Timespec,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .{ .timer = duration },
    };

    self.work_queue.push(task);
    return task;
}

pub fn cancelAll(self: *Uring) Allocator.Error!void {
    const task = try self.getTask();
    task.* = .{
        .userdata = null,
        .callback = io.noopCallback,
        .req = .{ .cancel = .all },
    };

    self.work_queue.push(task);
}

pub fn accept(
    self: *Uring,
    fd: posix.fd_t,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .{ .accept = fd },
    };

    self.work_queue.push(task);
    return task;
}

pub fn msgRing(
    self: *Uring,
    target: *const Uring,
    target_task: *io.Task, // The task that the target ring will receive. The callbacks of
    // this tsak are what will be called when the target receives the message
    result: u16, // We only allow sending a successful result
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    // This is the task to send the message
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .{ .msg_ring = .{
            .target = target,
            .result = result,
            .task = target_task,
        } },
    };
    target_task.state = .in_flight;
    self.work_queue.push(task);
    return task;
}

pub fn recv(
    self: *Uring,
    fd: posix.fd_t,
    buffer: []u8,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .{ .recv = .{
            .fd = fd,
            .buffer = buffer,
        } },
    };

    self.work_queue.push(task);
    return task;
}

pub fn write(
    self: *Uring,
    fd: posix.fd_t,
    buffer: []const u8,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .{ .write = .{
            .fd = fd,
            .buffer = buffer,
        } },
    };

    self.work_queue.push(task);
    return task;
}

pub fn writev(
    self: *Uring,
    fd: posix.fd_t,
    vecs: []const posix.iovec_const,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .{ .writev = .{
            .fd = fd,
            .vecs = vecs,
        } },
    };

    self.work_queue.push(task);
    return task;
}

pub fn close(
    self: *Uring,
    fd: posix.fd_t,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .{ .close = fd },
    };

    self.work_queue.push(task);
    return task;
}

pub fn poll(
    self: *Uring,
    fd: posix.fd_t,
    mask: u32,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .callback = callback,
        .req = .{ .poll = .{ .fd = fd, .mask = mask } },
    };

    self.work_queue.push(task);
    return task;
}

/// Foo is only for testing
const Foo = struct {
    bar: usize = 0,

    fn callback(_: *io.Ring, task: *io.Task, _: io.Result) anyerror!void {
        const self = task.ptrCast(@This());
        self.bar += 1;
    }
};

fn unexpectedError(err: posix.E) posix.UnexpectedError {
    std.log.err("unexpected posix error: {}", .{err});
    return error.Unexpected;
}

test "uring: inflight" {
    const gpa = std.testing.allocator;
    var ring: Uring = try .init(gpa, 16);
    defer ring.deinit();

    var foo: Foo = .{};
    const task = try ring.noop(&foo, Foo.callback);
    try task.setDeadline(&ring, .{ .sec = 1 });

    try ring.submitAndWait();

    try std.testing.expect(task.state == .in_flight);
    try std.testing.expectEqual(2, ring.inflight);

    try ring.reapCompletions();
}

test "uring: deadline doesn't call user callback" {
    const gpa = std.testing.allocator;
    var ring: Uring = try .init(gpa, 16);
    defer ring.deinit();

    var foo: Foo = .{};
    const task = try ring.noop(&foo, Foo.callback);
    try task.setDeadline(&ring, .{ .sec = 1 });

    try ring.run(.until_done);

    // Callback only called once
    try std.testing.expectEqual(1, foo.bar);
}

test "uring: timeout" {
    const gpa = std.testing.allocator;
    var ring: Uring = try .init(gpa, 16);
    defer ring.deinit();

    var foo: Foo = .{};

    const delay = 1 * std.time.ns_per_ms;
    _ = try ring.timer(
        .{ .nsec = delay },
        &foo,
        Foo.callback,
    );

    const start = std.time.nanoTimestamp();
    try ring.run(.until_done);
    try std.testing.expect(start + delay < std.time.nanoTimestamp());
    try std.testing.expectEqual(1, foo.bar);
}

test "uring: cancel" {
    const gpa = std.testing.allocator;
    var ring: Uring = try .init(gpa, 16);
    defer ring.deinit();

    var foo: Foo = .{};

    const delay = 1 * std.time.ns_per_ms;
    const task = try ring.timer(
        .{ .nsec = delay },
        &foo,
        Foo.callback,
    );

    try task.cancel(&ring, null, io.noopCallback);

    const start = std.time.nanoTimestamp();
    try ring.run(.until_done);
    // Expect that we didn't delay long enough
    try std.testing.expect(start + delay > std.time.nanoTimestamp());
    try std.testing.expectEqual(1, foo.bar);
}
