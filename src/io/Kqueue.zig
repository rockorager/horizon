const Kqueue = @This();

const std = @import("std");
const builtin = @import("builtin");
const test_options = @import("test_options");
const use_mock_io = test_options.use_mock_io;

const io = @import("io.zig");

const Allocator = std.mem.Allocator;
const EV = std.c.EV;
const EVFILT = std.c.EVFILT;
const Queue = @import("queue.zig").Intrusive;
const assert = std.debug.assert;
const posix = std.posix;

gpa: Allocator,
kq: posix.fd_t,
/// unused tasks
free_list: Queue(io.Task, .free) = .{},
/// Tasks we need to prepare
work_queue: Queue(io.Task, .in_flight) = .{},
/// Items we have prepared and waiting to be put into kqueue
submission_queue: std.ArrayListUnmanaged(posix.Kevent) = .empty,

/// Queue for other kqueue instances to send "completion" tasks to this thread
msg_ring_queue: Queue(io.Task, .in_flight) = .{},
msg_ring_result_queue: std.ArrayListUnmanaged(u16) = .empty,
msg_ring_mutex: std.Thread.Mutex = .{},
msg_ring_task: ?*io.Task = null,

timers: std.ArrayListUnmanaged(Timer) = .empty,

inflight: usize = 0,
run_cond: io.RunCondition = .until_done,

events: [128]posix.Kevent = undefined,
event_idx: usize = 0,

const Timer = union(enum) {
    /// a deadline timer cancels a task if it fires
    deadline: struct {
        /// the deadline task. If the parent completes before the deadline, the parent will set the
        /// deadline task state to .free
        task: *io.Task,
        /// the task to cancel if the deadline expires
        parent: *io.Task,
    },

    /// a regular timer
    timeout: struct {
        task: *io.Task,
        added_ms: i64,
    },

    /// Timer expires in the return value milliseconds from now
    fn expiresInMs(self: Timer, now_abs: i64) i64 {
        switch (self) {
            .deadline => |deadline| {
                const ts = deadline.task.req.deadline;
                const expires_ms = ts.sec * std.time.ms_per_s + @divTrunc(ts.nsec, std.time.ns_per_ms);
                return expires_ms - now_abs;
            },

            // timeouts are relative, so we add the time it was added to the queue
            .timeout => |timeout| {
                const ts = timeout.task.req.timer;
                const expires_ms = ts.sec * std.time.ms_per_s +
                    @divTrunc(ts.nsec, std.time.ns_per_ms) +
                    timeout.added_ms;
                return expires_ms - now_abs;
            },
        }
    }

    /// returns a timespec suitable for a kevent timeout. Relative to now
    fn timespec(self: Timer) posix.timespec {
        const expires = self.expiresInMs(std.time.milliTimestamp());
        return .{ .sec = @divFloor(expires, 1000), .nsec = @mod(expires, 1000) * std.time.ns_per_ms };
    }

    fn lessThan(now_ms: i64, lhs: Timer, rhs: Timer) bool {
        // reverse sort (we want soonest expiring last)
        return lhs.expiresInMs(now_ms) > rhs.expiresInMs(now_ms);
    }
};

const UserMsg = enum {
    wakeup,

    fn fromInt(v: i64) UserMsg {
        return @enumFromInt(v);
    }
};

/// Initialize a Ring
pub fn init(gpa: Allocator, _: u16) !Kqueue {
    const kq = try posix.kqueue();

    return .{
        .gpa = gpa,
        .kq = kq,
    };
}

pub fn deinit(self: *Kqueue) void {
    while (self.free_list.pop()) |task| self.gpa.destroy(task);
    while (self.work_queue.pop()) |task| self.gpa.destroy(task);
    while (self.msg_ring_queue.pop()) |task| self.gpa.destroy(task);

    self.submission_queue.deinit(self.gpa);
    self.msg_ring_result_queue.deinit(self.gpa);
    self.timers.deinit(self.gpa);
    if (self.msg_ring_task) |task| {
        self.gpa.destroy(task);
    }

    posix.close(self.kq);
    self.* = undefined;
}

/// Initializes a child Ring which can be woken up by self. This must be called from the thread
/// which will operate the child ring. Initializes with the same queue size as the parent
pub fn initChild(self: Kqueue, entries: u16) !Kqueue {
    return init(self.gpa, entries);
}

pub fn run(self: *Kqueue, limit: io.RunCondition) !void {
    self.run_cond = limit;

    if (self.msg_ring_task == null) {
        self.msg_ring_task = try self.getTask();
        // Register a user event we can use to wakeup the kqueue
        var kevent = evSet(
            @intFromEnum(UserMsg.wakeup),
            EVFILT.USER,
            EV.ADD | EV.CLEAR,
            self.msg_ring_task.?,
        );
        kevent.fflags = std.c.NOTE.FFNOP;
        try self.submission_queue.append(self.gpa, kevent);
    }

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

pub fn workQueueSize(self: Kqueue) usize {
    var count: usize = 0;
    var maybe_task: ?*io.Task = self.work_queue.head;
    while (maybe_task) |task| {
        count += 1;
        maybe_task = task.next;
    }
    return count;
}

/// Return a file descriptor which can be used to poll the ring for completions
pub fn pollableFd(self: Kqueue) !posix.fd_t {
    return self.kq;
}

pub fn reapCompletions(self: *Kqueue) anyerror!void {
    defer self.event_idx = 0;
    for (self.events[0..self.event_idx]) |event| {

        // if the event is a USER filter, we check our msg_ring_queue
        if (event.filter & EVFILT.USER != 0) {
            switch (UserMsg.fromInt(event.data)) {
                .wakeup => {
                    // We got a message in our msg_ring_queue
                    self.msg_ring_mutex.lock();
                    defer self.msg_ring_mutex.unlock();

                    while (self.msg_ring_queue.pop()) |task| {
                        const result = self.msg_ring_result_queue.orderedRemove(0);
                        var ev = event;
                        ev.data = result;
                        try self.handleCompletion(task, ev);
                    }
                },
            }
            continue;
        }

        const task: *io.Task = @ptrFromInt(event.udata);
        try self.handleCompletion(task, event);
    }
}

fn dataToE(result: i64) std.posix.E {
    if (result > 0) {
        return @as(std.posix.E, @enumFromInt(-result));
    }
    return .SUCCESS;
}

fn unexpectedError(err: posix.E) posix.UnexpectedError {
    std.log.err("unexpected posix error: {}", .{err});
    return error.Unexpected;
}

fn handleCompletion(self: *Kqueue, task: *io.Task, event: posix.Kevent) !void {
    // If a task was canceled, it's state will not be in_flight. Early return
    if (task.state != .in_flight) return;
    switch (task.req) {
        .accept => |req| {
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                return task.callback(task.userdata, self, task.msg, .{ .accept = err });
            }
            const fd = posix.accept(req, null, null, 0) catch {
                return task.callback(task.userdata, self, task.msg, .{ .accept = error.Unexpected });
            };
            return task.callback(task.userdata, self, task.msg, .{ .accept = fd });
        },

        // handled synchronously
        .cancel => unreachable,

        // handled synchronously
        .close => unreachable,

        .connect => {
            defer self.releaseTask(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                return task.callback(task.userdata, self, task.msg, .{ .connect = err });
            }
            return task.callback(task.userdata, self, task.msg, .{ .connect = {} });
        },

        // handled separately
        .deadline => unreachable,

        .msg_ring => |req| {
            defer self.releaseTask(task);
            const target = req.target;

            {
                target.msg_ring_mutex.lock();
                defer target.msg_ring_mutex.unlock();
                target.msg_ring_queue.push(req.task);

                target.msg_ring_result_queue.append(target.gpa, req.result) catch {
                    // On error we callback msgring parent task
                    try task.callback(task.userdata, self, task.msg, .{ .msg_ring = error.Unexpected });
                };
            }

            // wake up the other ring
            var kevent = evSet(
                @intFromEnum(UserMsg.wakeup),
                EVFILT.USER,
                0,
                null,
            );
            kevent.fflags |= std.c.NOTE.TRIGGER;
            // Trigger the wakeup
            _ = try posix.kevent(target.kq, &.{kevent}, &.{}, null);
        },

        // handled synchronously
        .noop => unreachable,

        .poll => {
            defer self.releaseTask(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                return task.callback(task.userdata, self, task.msg, .{ .poll = err });
            }
            return task.callback(task.userdata, self, task.msg, .{ .poll = {} });
        },

        .recv => |req| {
            defer self.releaseTask(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                return task.callback(task.userdata, self, task.msg, .{ .recv = err });
            }
            const n = posix.recv(req.fd, req.buffer, 0) catch {
                return task.callback(task.userdata, self, task.msg, .{ .recv = error.Unexpected });
            };
            return task.callback(task.userdata, self, task.msg, .{ .recv = n });
        },

        // handled synchronously
        .socket => unreachable,

        // handled separately
        .timer => unreachable,

        .usermsg => {
            defer self.releaseTask(task);
            try task.callback(task.userdata, self, task.msg, .{ .usermsg = @intCast(event.data) });
        },

        // Should never reach the runtime
        .userfd, .userptr => unreachable,

        .write => |req| {
            defer self.releaseTask(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                return task.callback(task.userdata, self, task.msg, .{ .write = err });
            }
            const n = posix.write(req.fd, req.buffer) catch {
                return task.callback(task.userdata, self, task.msg, .{ .write = error.Unexpected });
            };
            return task.callback(task.userdata, self, task.msg, .{ .write = n });
        },

        .writev => |req| {
            defer self.releaseTask(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                return task.callback(task.userdata, self, task.msg, .{ .writev = err });
            }
            const n = posix.writev(req.fd, req.vecs) catch {
                return task.callback(task.userdata, self, task.msg, .{ .writev = error.Unexpected });
            };
            return task.callback(task.userdata, self, task.msg, .{ .writev = n });
        },
    }
}

pub fn submitAndWait(self: *Kqueue) !void {
    defer self.submission_queue.clearRetainingCapacity();
    const tail = self.work_queue.tail;
    while (self.work_queue.pop()) |task| {
        task.next = null;
        try self.prepTask(task);

        // If this task is queued and has a deadline we need to schedule a timer
        if (task.state == .in_flight and task.deadline != null) {
            const deadline = task.deadline.?;
            try self.addTimer(.{ .deadline = .{ .task = deadline, .parent = task } });
        }

        // We make sure we never go past the tail we had when we started. We do this to break a
        // possible infinite loop for tasks which only ever loop through synchronous ops. By
        // breaking at the tail, we don't go past the submissions we had when we started to submit,
        // giving asynchronous ops a chance to be reaped
        if (task == tail.?) break;
    }

    // Sort our timers
    const now = std.time.milliTimestamp();
    std.sort.insertion(Timer, self.timers.items, now, Timer.lessThan);

    if (self.work_queue.empty()) {
        // We exhausted our work queue. We have to wait for some completion events
        return self.waitForCompletions();
    }

    // We already have completions from synchronous tasks. Submit our queued events and grab any new
    // completions for processing. We do so with a 0 timeout so that we are only grabbing already
    // completed items
    const timeout: posix.timespec = .{ .sec = 0, .nsec = 0 };
    self.inflight += self.submission_queue.items.len;
    self.event_idx = try posix.kevent(self.kq, self.submission_queue.items, &self.events, &timeout);
}

fn waitForCompletions(self: *Kqueue) !void {
    self.inflight += self.submission_queue.items.len;

    const now = std.time.milliTimestamp();
    // Go through our times until the first unexpired one
    while (true) {
        const t = self.timers.getLastOrNull() orelse break;
        if (t.expiresInMs(now) <= 0) {
            // timer expired. H
            _ = self.timers.pop();
            try self.handleExpiredTimer(t);
            continue;
        }

        const timeout: posix.timespec = t.timespec();

        self.event_idx = try posix.kevent(
            self.kq,
            self.submission_queue.items,
            &self.events,
            &timeout,
        );

        // if we had no returned events, it's because our timer expired
        if (self.event_idx == 0) {
            _ = self.timers.pop();
            try self.handleExpiredTimer(t);
            return;
        }
    }
}

fn releaseTask(self: *Kqueue, task: *io.Task) void {
    self.inflight -= 1;
    task.next = null;
    if (task.deadline) |d| d.state = .free;
    self.free_list.push(task);
}

fn handleExpiredTimer(self: *Kqueue, t: Timer) !void {
    switch (t) {
        .deadline => |deadline| {
            if (deadline.task.state == .free) {
                // The parent completed, we can return this to the freelist
                self.releaseTask(deadline.task);
                return;
            }

            const parent = deadline.parent;
            parent.state = .canceled;
            try parent.cancel(self, null, 0, io.noopCallback);
        },

        .timeout => |timeout| {
            const task = timeout.task;
            defer self.releaseTask(task);
            try task.callback(task.userdata, self, task.msg, .{ .timer = {} });
        },
    }
}

/// preps a task to be submitted into the kqueue
fn prepTask(self: *Kqueue, task: *io.Task) !void {
    return switch (task.req) {
        .accept => |req| {
            const kevent = evSet(@intCast(req), EVFILT.READ, EV.ADD | EV.ENABLE, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .cancel => |req| {
            defer self.releaseTask(task);
            switch (req) {
                .all => @panic("todo"),

                .task => |cancel_task| {
                    // We return the canceled task back to the free_list here. kqueue doesn't give
                    // us any returns for canceled tasks so we return them here
                    defer self.releaseTask(cancel_task);

                    switch (cancel_task.req) {
                        .accept => |cancel_req| {
                            const kevent = evSet(@intCast(cancel_req), EVFILT.READ, EV.DELETE, cancel_task);
                            try self.submission_queue.append(self.gpa, kevent);
                            try cancel_task.callback(
                                cancel_task.userdata,
                                self,
                                cancel_task.msg,
                                .{ .accept = error.Canceled },
                            );
                        },

                        // We don't cancel a cancel
                        .cancel => {},

                        // Nothing to do, these are synchronous
                        .close => {},

                        .connect => |cancel_req| {
                            const kevent = evSet(
                                @intCast(cancel_req.fd),
                                EVFILT.WRITE,
                                EV.DELETE,
                                cancel_task,
                            );
                            try self.submission_queue.append(self.gpa, kevent);
                            try cancel_task.callback(
                                cancel_task.userdata,
                                self,
                                cancel_task.msg,
                                .{ .connect = error.Canceled },
                            );
                        },

                        .deadline => {
                            // What does it mean to cancel a deadline? We remove the deadline from
                            // the parent and the timer from our list
                            for (self.timers.items, 0..) |t, i| {
                                if (t == .deadline and t.deadline.task == cancel_task) {
                                    // Set the parent deadline to null
                                    t.deadline.parent.deadline = null;
                                    // Remove the timer
                                    _ = self.timers.orderedRemove(i);
                                    return;
                                }
                            }
                        },

                        .msg_ring => @panic("todo"),

                        // handled synchronously
                        .noop => {},

                        .poll => |cancel_req| {
                            if (cancel_req.mask & posix.POLL.IN != 0) {
                                const kevent = evSet(@intCast(cancel_req.fd), EVFILT.READ, EV.DELETE, task);
                                try self.submission_queue.append(self.gpa, kevent);
                            }
                            if (cancel_req.mask & posix.POLL.OUT != 0) {
                                const kevent = evSet(@intCast(cancel_req.fd), EVFILT.WRITE, EV.DELETE, task);
                                try self.submission_queue.append(self.gpa, kevent);
                            }
                            try cancel_task.callback(
                                cancel_task.userdata,
                                self,
                                cancel_task.msg,
                                .{ .poll = error.Canceled },
                            );
                        },

                        .recv => |cancel_req| {
                            const kevent = evSet(
                                @intCast(cancel_req.fd),
                                EVFILT.READ,
                                EV.DELETE,
                                cancel_task,
                            );
                            try self.submission_queue.append(self.gpa, kevent);
                            try cancel_task.callback(
                                cancel_task.userdata,
                                self,
                                cancel_task.msg,
                                .{ .recv = error.Canceled },
                            );
                        },

                        // handled synchronously
                        .socket => {},

                        .timer => {
                            for (self.timers.items, 0..) |t, i| {
                                if (t == .timeout and t.timeout.task == cancel_task) {
                                    // Remove the timer
                                    _ = self.timers.orderedRemove(i);
                                    return;
                                }
                            }
                            try cancel_task.callback(
                                cancel_task.userdata,
                                self,
                                cancel_task.msg,
                                .{ .timer = error.Canceled },
                            );
                        },

                        // Never cancelable
                        .userfd, .usermsg, .userptr => {},

                        .write => |cancel_req| {
                            const kevent = evSet(
                                @intCast(cancel_req.fd),
                                EVFILT.WRITE,
                                EV.DELETE,
                                cancel_task,
                            );
                            try self.submission_queue.append(self.gpa, kevent);
                            try cancel_task.callback(
                                cancel_task.userdata,
                                self,
                                cancel_task.msg,
                                .{ .write = error.Canceled },
                            );
                        },

                        .writev => |cancel_req| {
                            const kevent = evSet(
                                @intCast(cancel_req.fd),
                                EVFILT.WRITE,
                                EV.DELETE,
                                cancel_task,
                            );
                            try self.submission_queue.append(self.gpa, kevent);
                            try cancel_task.callback(
                                cancel_task.userdata,
                                self,
                                cancel_task.msg,
                                .{ .writev = error.Canceled },
                            );
                        },
                    }
                },
            }
        },

        .close => |req| {
            defer self.free_list.push(task);
            posix.close(req);
            try task.callback(task.userdata, self, task.msg, .{ .close = {} });
        },

        .connect => |req| {
            // Set nonblocking. Call connect. Then add it to the kqueue. This will return as
            // writeable when the connect is complete
            const arg: posix.O = .{ .NONBLOCK = true };
            const arg_u32: u32 = @bitCast(arg);
            _ = posix.fcntl(req.fd, posix.F.SETFL, arg_u32) catch {
                defer self.free_list.push(task);
                try task.callback(task.userdata, self, task.msg, .{ .connect = error.Unexpected });
            };

            if (posix.connect(req.fd, req.addr, req.addr_len)) {
                // We connected immediately. No need to add to kqueue
                defer self.free_list.push(task);
                try task.callback(task.userdata, self, task.msg, .{ .connect = {} });
            } else |err| {
                switch (err) {
                    error.WouldBlock => {
                        // This is the error we expect. Add the event to kqueue
                        const kevent = evSet(@intCast(req.fd), EVFILT.WRITE, EV.ADD | EV.ENABLE, task);
                        try self.submission_queue.append(self.gpa, kevent);
                    },
                    else => return err,
                }
            }
        },

        // deadlines are handled separately
        .deadline => unreachable,

        .msg_ring => @panic("TODO"),

        // noop just calls the callback right away
        .noop => {
            defer self.free_list.push(task);
            try task.callback(task.userdata, self, task.msg, .noop);
        },

        .poll => |req| {
            if (req.mask & posix.POLL.IN != 0) {
                const kevent = evSet(@intCast(req.fd), EVFILT.READ, EV.ADD | EV.ENABLE | EV.CLEAR, task);
                try self.submission_queue.append(self.gpa, kevent);
            }
            if (req.mask & posix.POLL.OUT != 0) {
                const kevent = evSet(@intCast(req.fd), EVFILT.WRITE, EV.ADD | EV.ENABLE | EV.CLEAR, task);
                try self.submission_queue.append(self.gpa, kevent);
            }
        },

        .recv => |req| {
            const kevent = evSet(@intCast(req.fd), EVFILT.READ, EV.ADD | EV.ENABLE | EV.CLEAR, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .socket => |req| {
            defer self.free_list.push(task);
            const fd = posix.socket(req.domain, req.type, req.protocol) catch {
                try task.callback(task.userdata, self, task.msg, .{ .socket = error.Unexpected });
                return;
            };
            try task.callback(task.userdata, self, task.msg, .{ .socket = fd });
        },

        .timer => {
            const now = std.time.milliTimestamp();
            try self.addTimer(.{ .timeout = .{ .task = task, .added_ms = now } });
        },

        // user* fields are never seen by the runtime, only for internal message passing
        .userfd, .usermsg, .userptr => unreachable,

        .write => |req| {
            const kevent = evSet(@intCast(req.fd), EVFILT.WRITE, EV.ADD | EV.ENABLE, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .writev => |req| {
            const kevent = evSet(@intCast(req.fd), EVFILT.WRITE, EV.ADD | EV.ENABLE, task);
            try self.submission_queue.append(self.gpa, kevent);
        },
    };
}

fn evSet(ident: usize, filter: i16, flags: u16, ptr: ?*anyopaque) posix.Kevent {
    return switch (builtin.os.tag) {
        .netbsd,
        .dragonfly,
        .openbsd,
        .macos,
        .ios,
        .tvos,
        .watchos,
        .visionos,
        => .{
            .ident = ident,
            .filter = filter,
            .flags = flags,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(ptr),
        },

        .freebsd => .{
            .ident = ident,
            .filter = filter,
            .flags = flags,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(ptr),
            ._ext = &.{ 0, 0, 0, 0 },
        },

        else => @compileError("kqueue not supported"),
    };
}

pub fn submit(self: *Kqueue) !void {
    defer self.submission_queue.clearRetainingCapacity();
    const tail = self.work_queue.tail;
    while (self.work_queue.pop()) |task| {
        task.next = null;
        try self.prepTask(task);

        // If this task is queued and has a deadline we need to schedule a timer
        if (task.state == .in_flight and task.deadline != null) {
            const deadline = task.deadline.?;
            try self.addTimer(.{ .deadline = .{ .task = deadline, .parent = task } });
        }

        // We make sure we never go past the tail we had when we started. We do this to break a
        // possible infinite loop for tasks which only ever loop through synchronous ops. By
        // breaking at the tail, we don't go past the submissions we had when we started to submit,
        // giving asynchronous ops a chance to be reaped
        if (task == tail.?) break;
    }

    // Sort our timers
    const now = std.time.milliTimestamp();
    std.sort.insertion(Timer, self.timers.items, now, Timer.lessThan);

    const timeout: posix.timespec = .{ .sec = 0, .nsec = 0 };
    self.inflight += self.submission_queue.items.len;
    self.event_idx = try posix.kevent(self.kq, self.submission_queue.items, &self.events, &timeout);
}

pub fn getTask(self: *Kqueue) Allocator.Error!*io.Task {
    return self.free_list.pop() orelse try self.gpa.create(io.Task);
}

pub fn noop(
    self: *Kqueue,
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
    self: *Kqueue,
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

fn addTimer(self: *Kqueue, t: Timer) !void {
    try self.timers.append(self.gpa, t);
}

pub fn cancelAll(self: *Kqueue) Allocator.Error!void {
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
    self: *Kqueue,
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
    self: *Kqueue,
    target: *Kqueue,
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
    self: *Kqueue,
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
    self: *Kqueue,
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
    self: *Kqueue,
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
    self: *Kqueue,
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
    self: *Kqueue,
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
    self: *Kqueue,
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
    self: *Kqueue,
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

test "kqueue: noop" {
    std.testing.refAllDecls(@This());

    var rt: Kqueue = try .init(std.testing.allocator, 0);
    defer rt.deinit();

    const Foo = struct {
        val: usize = 0,
        fn callback(ptr: ?*anyopaque, _: *io.Runtime, _: u16, _: io.Result) anyerror!void {
            const self = io.ptrCast(@This(), ptr);
            self.val += 1;
        }
    };

    var foo: Foo = .{};

    _ = try rt.noop(&foo, 0, Foo.callback);
    try rt.run(.once);
    try std.testing.expectEqual(1, foo.val);
    _ = try rt.noop(&foo, 0, Foo.callback);
    _ = try rt.noop(&foo, 0, Foo.callback);
    try rt.run(.once);
    try std.testing.expectEqual(3, foo.val);
}

test "kqueue: timer" {
    std.testing.refAllDecls(@This());

    var rt: Kqueue = try .init(std.testing.allocator, 0);
    defer rt.deinit();

    const Foo = struct {
        val: usize = 0,
        fn callback(ptr: ?*anyopaque, _: *io.Runtime, _: u16, _: io.Result) anyerror!void {
            const self = io.ptrCast(@This(), ptr);
            self.val += 1;
        }
    };

    var foo: Foo = .{};

    const start = std.time.nanoTimestamp();
    const end = start + 100 * std.time.ns_per_ms;
    _ = try rt.timer(.{ .nsec = 100 * std.time.ns_per_ms }, &foo, 0, Foo.callback);
    try rt.run(.once);
    try std.testing.expect(std.time.nanoTimestamp() > end);
    try std.testing.expectEqual(1, foo.val);
}

test "kqueue: poll" {
    std.testing.refAllDecls(@This());

    var rt: Kqueue = try .init(std.testing.allocator, 0);
    defer rt.deinit();

    const Foo = struct {
        val: usize = 0,
        fn callback(ptr: ?*anyopaque, _: *io.Runtime, _: u16, result: io.Result) anyerror!void {
            _ = result.poll catch |err| return err;
            const self = io.ptrCast(@This(), ptr);
            self.val += 1;
        }
    };

    var foo: Foo = .{};
    const pipe = try posix.pipe2(.{ .CLOEXEC = true });

    _ = try rt.poll(pipe[0], posix.POLL.IN, &foo, 0, Foo.callback);
    try std.testing.expectEqual(1, rt.workQueueSize());

    try rt.submit();
    _ = try posix.write(pipe[1], "io_uring is better");
    try rt.reapCompletions();
    try std.testing.expectEqual(1, foo.val);
}
