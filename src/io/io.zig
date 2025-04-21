const std = @import("std");
const builtin = @import("builtin");
const test_options = @import("test_options");

const Allocator = std.mem.Allocator;
const Queue = @import("queue.zig").Intrusive;
const io = @This();
const posix = std.posix;

pub const has_kqueue = switch (builtin.os.tag) {
    .dragonfly,
    .freebsd,
    .ios,
    .macos,
    .netbsd,
    .openbsd,
    .tvos,
    .visionos,
    .watchos,
    => true,

    else => false,
};
pub const has_io_uring = builtin.os.tag == .linux;

/// True if the io runtime is being mocked. Useful for testing application logic
pub const use_mock = test_options.use_mock_io;
pub const Task = @import("Task.zig");
pub const Callback = *const fn (?*anyopaque, *Runtime, u16, Result) anyerror!void;
pub fn noopCallback(_: ?*anyopaque, _: *Runtime, _: u16, _: Result) anyerror!void {}

pub fn ptrCast(comptime T: type, ptr: ?*anyopaque) *T {
    return @ptrCast(@alignCast(ptr));
}

pub const RunCondition = enum {
    once,
    until_done,
    forever,
};

/// Used for timeouts and deadlines. We make this struct extern because we will ptrCast it to the
/// linux kernel timespec struct
pub const Timespec = extern struct {
    sec: i64 = 0,
    nsec: i64 = 0,

    pub fn isZero(self: Timespec) bool {
        return self.sec == 0 and self.nsec == 0;
    }
};

pub const Backend = union(enum) {
    mock: @import("MockRuntime.zig"),

    platform: switch (builtin.os.tag) {
        .dragonfly,
        .freebsd,
        .ios,
        .macos,
        .netbsd,
        .openbsd,
        .tvos,
        .visionos,
        .watchos,
        => @import("Kqueue.zig"),

        .linux => @import("Uring.zig"),

        else => @compileError("unsupported os"),
    },

    pub fn initChild(self: *Backend, entries: u16) !Backend {
        switch (self.*) {
            .mock => return .{ .mock = .{} },
            .platfrom => |*p| return .{ .platform = try p.initChild(entries) },
        }
    }

    pub fn deinit(self: *Backend, gpa: Allocator) void {
        switch (self.*) {
            inline else => |*backend| backend.deinit(gpa),
        }
    }

    pub fn submitAndWait(self: *Backend, queue: *SubmissionQueue) !void {
        return switch (self.*) {
            inline else => |*backend| backend.submitAndWait(queue),
        };
    }

    pub fn submit(self: *Backend, queue: *SubmissionQueue) !void {
        return switch (self.*) {
            inline else => |*backend| backend.submit(queue),
        };
    }

    pub fn reapCompletions(
        self: *Backend,
        rt: *Runtime,
    ) !void {
        return switch (self.*) {
            inline else => |*backend| backend.reapCompletions(rt),
        };
    }

    pub fn done(self: *Backend) bool {
        return switch (self.*) {
            inline else => |*backend| backend.done(),
        };
    }
};

pub const CompletionQueue = Queue(Task, .complete);
pub const FreeQueue = Queue(Task, .free);
pub const SubmissionQueue = Queue(Task, .in_flight);

pub const Runtime = struct {
    backend: Backend,
    gpa: Allocator,

    completion_q: CompletionQueue,
    submission_q: SubmissionQueue,
    free_q: FreeQueue,

    pub fn init(gpa: Allocator, entries: u16) !Runtime {
        return .{
            .backend = .{ .platform = try .init(gpa, entries) },
            .gpa = gpa,
            .free_q = .{},
            .submission_q = .{},
            .completion_q = .{},
        };
    }

    pub fn initMock(gpa: Allocator, entries: u16) !Runtime {
        return .{
            .backend = .{ .mock = try .init(gpa, entries) },
            .gpa = gpa,
            .free_q = .{},
            .submission_q = .{},
            .completion_q = .{},
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.backend.deinit(self.gpa);
        while (self.free_q.pop()) |task| self.gpa.destroy(task);
        while (self.submission_q.pop()) |task| self.gpa.destroy(task);
        while (self.completion_q.pop()) |task| self.gpa.destroy(task);
    }

    pub fn run(self: *Runtime, condition: RunCondition) !void {
        while (true) {
            try self.backend.submitAndWait(&self.submission_q);
            try self.backend.reapCompletions(self);
            while (self.completion_q.pop()) |task| {
                try task.callback(task.userdata, self, task.msg, task.result.?);
            }
            switch (condition) {
                .once => return,
                .until_done => if (self.backend.done() and self.submission_q.empty()) return,
                .forever => {},
            }
        }
    }

    pub fn getTask(self: *Runtime) Allocator.Error!*Task {
        return self.free_q.pop() orelse try self.gpa.create(Task);
    }

    pub fn noop(
        self: *Runtime,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .noop,
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn timer(
        self: *Runtime,
        duration: Timespec,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .timer = duration },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn cancelAll(self: *Runtime) Allocator.Error!void {
        const task = try self.getTask();
        task.* = .{
            .userdata = null,
            .msg = 0,
            .callback = noopCallback,
            .req = .{ .cancel = .all },
        };

        self.submission_q.push(task);
    }

    pub fn accept(
        self: *Runtime,
        fd: posix.fd_t,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .accept = fd },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn msgRing(
        self: *Runtime,
        target: *Runtime,
        target_task: *Task, // The task that the target ring will receive. The callbacks of
        // this tsak are what will be called when the target receives the message
        result: u16, // We only allow sending a successful result
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        // This is the task to send the message
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .msg_ring = .{
                .target = target,
                .result = result,
                .task = target_task,
            } },
        };
        target_task.state = .in_flight;
        self.submission_q.push(task);
        return task;
    }

    pub fn recv(
        self: *Runtime,
        fd: posix.fd_t,
        buffer: []u8,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .recv = .{
                .fd = fd,
                .buffer = buffer,
            } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn write(
        self: *Runtime,
        fd: posix.fd_t,
        buffer: []const u8,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .write = .{
                .fd = fd,
                .buffer = buffer,
            } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn writev(
        self: *Runtime,
        fd: posix.fd_t,
        vecs: []const posix.iovec_const,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .writev = .{
                .fd = fd,
                .vecs = vecs,
            } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn close(
        self: *Runtime,
        fd: posix.fd_t,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .close = fd },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn poll(
        self: *Runtime,
        fd: posix.fd_t,
        mask: u32,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .poll = .{ .fd = fd, .mask = mask } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn socket(
        self: *Runtime,
        domain: u32,
        socket_type: u32,
        protocol: u32,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .socket = .{ .domain = domain, .type = socket_type, .protocol = protocol } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn connect(
        self: *Runtime,
        fd: posix.socket_t,
        addr: *posix.sockaddr,
        addr_len: posix.socklen_t,
        userdata: ?*anyopaque,
        msg: u16,
        callback: Callback,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = userdata,
            .msg = msg,
            .callback = callback,
            .req = .{ .connect = .{ .fd = fd, .addr = addr, .addr_len = addr_len } },
        };

        self.submission_q.push(task);
        return task;
    }
};

pub const Op = enum {
    noop,
    deadline,
    timer,
    cancel,
    accept,
    msg_ring,
    recv,
    write,
    writev,
    close,
    poll,
    socket,
    connect,

    userfd,
    usermsg,
    userptr,
};

pub const Request = union(Op) {
    noop,
    deadline: Timespec,
    timer: Timespec,
    cancel: union(enum) {
        all,
        task: *Task,
    },
    accept: posix.fd_t,
    msg_ring: struct {
        target: *Runtime,
        result: u16,
        task: *Task,
    },
    recv: struct {
        fd: posix.fd_t,
        buffer: []u8,
    },
    write: struct {
        fd: posix.fd_t,
        buffer: []const u8,
    },
    writev: struct {
        fd: posix.fd_t,
        vecs: []const posix.iovec_const,
    },
    close: posix.fd_t,
    poll: struct {
        fd: posix.fd_t,
        mask: u32,
    },
    socket: struct {
        domain: u32,
        type: u32,
        protocol: u32,
    },
    connect: struct {
        fd: posix.socket_t,
        addr: *posix.sockaddr,
        addr_len: posix.socklen_t,
    },

    userfd,
    usermsg,
    userptr,
};

pub const Result = union(Op) {
    noop,
    deadline: ResultError!void,
    timer: ResultError!void,
    cancel: CancelError!void,
    accept: ResultError!posix.fd_t,
    msg_ring: ResultError!void,
    recv: RecvError!usize,
    write: ResultError!usize,
    writev: ResultError!usize,
    close: ResultError!void,
    poll: ResultError!void,
    socket: ResultError!posix.fd_t,
    connect: ResultError!void,

    userfd: anyerror!posix.fd_t,
    usermsg: u16,
    userptr: anyerror!?*anyopaque,
};

pub const ResultError = error{
    /// The request was invalid
    Invalid,
    /// The request was canceled
    Canceled,
    /// An unexpected error occured
    Unexpected,
};

pub const CancelError = ResultError || error{
    /// The entry to cancel couldn't be found
    EntryNotFound,
    /// The entry couldn't be canceled
    NotCanceled,
};

pub const RecvError = ResultError || error{
    /// The entry to cancel couldn't be found
    ConnectionResetByPeer,
};

test {
    _ = @import("net.zig");
    _ = @import("queue.zig");

    _ = @import("MockRuntime.zig");

    if (has_io_uring) _ = @import("Uring.zig");
    if (has_kqueue) _ = @import("Kqueue.zig");
}

/// Foo is only for testing
const Foo = struct {
    bar: usize = 0,

    fn callback(ptr: ?*anyopaque, _: *io.Runtime, _: u16, _: io.Result) anyerror!void {
        const self = io.ptrCast(@This(), ptr);
        self.bar += 1;
    }
};

test "runtime: noop" {
    var rt: io.Runtime = try .init(std.testing.allocator, 16);
    defer rt.deinit();

    var foo: Foo = .{};

    // noop is triggered synchronously with submit. If we wait, we'll be waiting forever
    _ = try rt.noop(&foo, 0, Foo.callback);
    try rt.run(.once);
    try std.testing.expectEqual(1, foo.bar);
    _ = try rt.noop(&foo, 0, Foo.callback);
    _ = try rt.noop(&foo, 0, Foo.callback);
    try rt.run(.once);
    try std.testing.expectEqual(3, foo.bar);
}

test "runtime: timer" {
    var rt: io.Runtime = try .init(std.testing.allocator, 16);
    defer rt.deinit();

    var foo: Foo = .{};

    const start = std.time.nanoTimestamp();
    const end = start + 100 * std.time.ns_per_ms;
    _ = try rt.timer(.{ .nsec = 100 * std.time.ns_per_ms }, &foo, 0, Foo.callback);
    try rt.run(.once);
    try std.testing.expect(std.time.nanoTimestamp() > end);
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: poll" {
    var rt: io.Runtime = try .init(std.testing.allocator, 16);
    defer rt.deinit();

    var foo: Foo = .{};
    const pipe = try posix.pipe2(.{ .CLOEXEC = true });

    _ = try rt.poll(pipe[0], posix.POLL.IN, &foo, 0, Foo.callback);
    try std.testing.expectEqual(1, rt.submission_q.len());

    _ = try posix.write(pipe[1], "io_uring is the best");
    try rt.run(.once);
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: deadline doesn't call user callback" {
    const gpa = std.testing.allocator;
    var rt = try io.Runtime.init(gpa, 16);
    defer rt.deinit();

    var foo: Foo = .{};
    const task = try rt.noop(&foo, 0, Foo.callback);
    try task.setDeadline(&rt, .{ .sec = 1 });

    try rt.run(.until_done);

    // Callback only called once
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: timeout" {
    const gpa = std.testing.allocator;
    var rt = try io.Runtime.init(gpa, 16);
    defer rt.deinit();

    var foo: Foo = .{};

    const delay = 1 * std.time.ns_per_ms;
    _ = try rt.timer(
        .{ .nsec = delay },
        &foo,
        0,
        Foo.callback,
    );

    const start = std.time.nanoTimestamp();
    try rt.run(.until_done);
    try std.testing.expect(start + delay < std.time.nanoTimestamp());
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: cancel" {
    const gpa = std.testing.allocator;
    var rt = try io.Runtime.init(gpa, 16);
    defer rt.deinit();

    var foo: Foo = .{};

    const delay = 1 * std.time.ns_per_s;
    const task = try rt.timer(
        .{ .nsec = delay },
        &foo,
        0,
        Foo.callback,
    );

    try task.cancel(&rt, null, 0, io.noopCallback);

    const start = std.time.nanoTimestamp();
    try rt.run(.until_done);
    // Expect that we didn't delay long enough
    try std.testing.expect(start + delay > std.time.nanoTimestamp());
    try std.testing.expectEqual(1, foo.bar);
}
