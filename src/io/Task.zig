const Task = @This();

const std = @import("std");
const io = @import("io.zig");

const Allocator = std.mem.Allocator;
const Runtime = io.Runtime;

userdata: ?*anyopaque,
msg: u16,
callback: io.Callback,
req: io.Request,

state: enum {
    /// The task is free to be scheduled
    free,

    /// The task is in flight and may not be rescheduled. Some operations generate multiple
    /// completions, so it is possible to receive a task in Callback and the task is still
    /// considered to be in flight
    in_flight,
} = .free,

/// Deadline for the task to complete, in absolute time. If 0, there is no deadline
deadline: ?*Task = null,

next: ?*Task = null,

pub fn setDeadline(
    self: *Task,
    rt: *Runtime,
    deadline: io.Timespec,
) Allocator.Error!void {
    std.debug.assert(!deadline.isZero());
    const task = try rt.getTask();

    task.* = .{
        .callback = io.noopCallback,
        .userdata = null,
        .msg = 0,
        .req = .{ .deadline = deadline },
    };

    self.deadline = task;
}

pub fn cancel(
    self: *Task,
    rt: *Runtime,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!void {
    const task = try rt.getTask();
    task.* = .{
        .callback = callback,
        .msg = msg,
        .userdata = userdata,
        .req = .{ .cancel = .{ .task = self } },
    };
    rt.work_queue.push(task);
}
