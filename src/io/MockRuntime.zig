const MockRuntime = @This();

const std = @import("std");
const test_options = @import("test_options");
const use_mock_io = test_options.use_mock_io;

const io = @import("io.zig");

const Allocator = std.mem.Allocator;
const Queue = @import("queue.zig").Intrusive;
const assert = std.debug.assert;
const posix = std.posix;

gpa: Allocator,
free_list: Queue(io.Task, .free) = .{},
work_queue: Queue(io.Task, .in_flight) = .{},
inflight: usize = 0,
run_cond: io.RunCondition = .until_done,

accept_cb: ?*const fn (*io.Task) io.Result = null,
cancel_cb: ?*const fn (*io.Task) io.Result = null,
close_cb: ?*const fn (*io.Task) io.Result = null,
connect_cb: ?*const fn (*io.Task) io.Result = null,
deadline_cb: ?*const fn (*io.Task) io.Result = null,
msg_ring_cb: ?*const fn (*io.Task) io.Result = null,
noop_cb: ?*const fn (*io.Task) io.Result = null,
poll_cb: ?*const fn (*io.Task) io.Result = null,
recv_cb: ?*const fn (*io.Task) io.Result = null,
socket_cb: ?*const fn (*io.Task) io.Result = null,
timer_cb: ?*const fn (*io.Task) io.Result = null,
write_cb: ?*const fn (*io.Task) io.Result = null,
writev_cb: ?*const fn (*io.Task) io.Result = null,

userfd_cb: ?*const fn (*io.Task) io.Result = null,
usermsg_cb: ?*const fn (*io.Task) io.Result = null,
userptr_cb: ?*const fn (*io.Task) io.Result = null,

/// Initialize a Ring
pub fn init(gpa: Allocator, _: u16) !MockRuntime {
    return .{ .gpa = gpa };
}

pub fn deinit(self: *MockRuntime) void {
    while (self.free_list.pop()) |task| self.gpa.destroy(task);
    while (self.work_queue.pop()) |task| self.gpa.destroy(task);
    self.* = undefined;
}

/// Initializes a child Ring which can be woken up by self. This must be called from the thread
/// which will operate the child ring. Initializes with the same queue size as the parent
pub fn initChild(self: MockRuntime, entries: u16) !MockRuntime {
    return init(self.gpa, entries);
}

pub fn run(self: *MockRuntime, limit: io.RunCondition) !void {
    self.run_cond = limit;
    while (true) {
        try self.submitAndWait();
        try self.reapCompletions();
        switch (self.run_cond) {
            .once => return,
            .until_done => if (self.inflight == 0 and self.work_queue.empty()) return,
            .forever => {},
        }
    }
}

pub fn workQueueSize(self: MockRuntime) usize {
    var count: usize = 0;
    var maybe_task: ?*io.Task = self.work_queue.head;
    while (maybe_task) |task| {
        count += 1;
        maybe_task = task.next;
    }
    return count;
}

/// Return a file descriptor which can be used to poll the ring for completions
pub fn pollableFd(_: *MockRuntime) !posix.fd_t {
    return -1;
}

pub fn msgRingFd(_: MockRuntime) posix.fd_t {
    return -1;
}

pub fn reapCompletions(self: *MockRuntime) anyerror!void {
    while (self.work_queue.pop()) |task| {
        const result = switch (task.req) {
            .accept => if (self.accept_cb) |cb| cb(task) else return error.NoMockCallback,
            .cancel => if (self.cancel_cb) |cb| cb(task) else return error.NoMockCallback,
            .close => if (self.close_cb) |cb| cb(task) else return error.NoMockCallback,
            .connect => if (self.connect_cb) |cb| cb(task) else return error.NoMockCallback,
            .deadline => if (self.deadline_cb) |cb| cb(task) else return error.NoMockCallback,
            .msg_ring => if (self.msg_ring_cb) |cb| cb(task) else return error.NoMockCallback,
            .noop => if (self.noop_cb) |cb| cb(task) else return error.NoMockCallback,
            .poll => if (self.poll_cb) |cb| cb(task) else return error.NoMockCallback,
            .recv => if (self.recv_cb) |cb| cb(task) else return error.NoMockCallback,
            .socket => if (self.socket_cb) |cb| cb(task) else return error.NoMockCallback,
            .timer => if (self.timer_cb) |cb| cb(task) else return error.NoMockCallback,
            .userfd => if (self.userfd_cb) |cb| cb(task) else return error.NoMockCallback,
            .usermsg => if (self.usermsg_cb) |cb| cb(task) else return error.NoMockCallback,
            .userptr => if (self.userptr_cb) |cb| cb(task) else return error.NoMockCallback,
            .write => if (self.write_cb) |cb| cb(task) else return error.NoMockCallback,
            .writev => if (self.writev_cb) |cb| cb(task) else return error.NoMockCallback,
        };
        try task.callback(task.userdata, self, task.msg, result);
        self.free_list.push(task);
    }
}

pub fn submitAndWait(_: *MockRuntime) !void {}

pub fn submit(_: *MockRuntime) !void {}

pub fn prepTask(_: *MockRuntime, _: *io.Task) void {}

pub fn getTask(self: *MockRuntime) Allocator.Error!*io.Task {
    return self.free_list.pop() orelse try self.gpa.create(io.Task);
}

pub fn noop(
    self: *MockRuntime,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .msg = msg,
        .callback = callback,
        .req = .noop,
    };

    self.work_queue.push(task);
    return task;
}

pub fn timer(
    self: *MockRuntime,
    duration: io.Timespec,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .msg = msg,
        .callback = callback,
        .req = .{ .timer = duration },
    };

    self.work_queue.push(task);
    return task;
}

pub fn cancelAll(self: *MockRuntime) Allocator.Error!void {
    const task = try self.getTask();
    task.* = .{
        .userdata = null,
        .msg = 0,
        .callback = io.noopCallback,
        .req = .{ .cancel = .all },
    };

    self.work_queue.push(task);
}

pub fn accept(
    self: *MockRuntime,
    fd: posix.fd_t,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .msg = msg,
        .callback = callback,
        .req = .{ .accept = fd },
    };

    self.work_queue.push(task);
    return task;
}

pub fn msgRing(
    self: *MockRuntime,
    target: *const MockRuntime,
    target_task: *io.Task, // The task that the target ring will receive. The callbacks of
    // this tsak are what will be called when the target receives the message
    result: u16, // We only allow sending a successful result
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
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
    self.work_queue.push(task);
    return task;
}

pub fn recv(
    self: *MockRuntime,
    fd: posix.fd_t,
    buffer: []u8,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
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

    self.work_queue.push(task);
    return task;
}

pub fn write(
    self: *MockRuntime,
    fd: posix.fd_t,
    buffer: []const u8,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
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

    self.work_queue.push(task);
    return task;
}

pub fn writev(
    self: *MockRuntime,
    fd: posix.fd_t,
    vecs: []const posix.iovec_const,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
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

    self.work_queue.push(task);
    return task;
}

pub fn close(
    self: *MockRuntime,
    fd: posix.fd_t,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .msg = msg,
        .callback = callback,
        .req = .{ .close = fd },
    };

    self.work_queue.push(task);
    return task;
}

pub fn poll(
    self: *MockRuntime,
    fd: posix.fd_t,
    mask: u32,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .msg = msg,
        .callback = callback,
        .req = .{ .poll = .{ .fd = fd, .mask = mask } },
    };

    self.work_queue.push(task);
    return task;
}

pub fn socket(
    self: *MockRuntime,
    domain: u32,
    socket_type: u32,
    protocol: u32,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .msg = msg,
        .callback = callback,
        .req = .{ .socket = .{ .domain = domain, .type = socket_type, .protocol = protocol } },
    };

    self.work_queue.push(task);
    return task;
}

pub fn connect(
    self: *MockRuntime,
    fd: posix.socket_t,
    addr: *posix.sockaddr,
    addr_len: posix.socklen_t,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*io.Task {
    const task = try self.getTask();
    task.* = .{
        .userdata = userdata,
        .msg = msg,
        .callback = callback,
        .req = .{ .connect = .{ .fd = fd, .addr = addr, .addr_len = addr_len } },
    };

    self.work_queue.push(task);
    return task;
}
