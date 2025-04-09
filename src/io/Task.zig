const Task = @This();

const std = @import("std");
const io = @import("io.zig");

const Allocator = std.mem.Allocator;
const Ring = io.Ring;

userdata: ?*anyopaque,
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

pub fn ptrCast(self: Task, comptime T: type) *T {
    return @ptrCast(@alignCast(self.userdata));
}

pub fn setDeadline(
    self: *Task,
    ring: *Ring,
    deadline: io.Timespec,
) Allocator.Error!void {
    std.debug.assert(!deadline.isZero());
    const task = try ring.getTask();

    task.* = .{
        .callback = io.noopCallback,
        .userdata = null,
        .req = .{ .deadline = deadline },
    };

    self.deadline = task;
}

pub fn cancel(
    self: *Task,
    ring: *Ring,
    userdata: ?*anyopaque,
    callback: io.Callback,
) Allocator.Error!void {
    const task = try ring.getTask();
    task.* = .{
        .callback = callback,
        .userdata = userdata,
        .req = .{ .cancel = .{ .task = self } },
    };
    ring.work_queue.push(task);
}
