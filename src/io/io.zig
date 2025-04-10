const std = @import("std");
const builtin = @import("builtin");

const posix = std.posix;

pub const Task = @import("Task.zig");
pub const Callback = *const fn (*Ring, *Task, Result) anyerror!void;
pub fn noopCallback(_: *Ring, _: *Task, _: Result) anyerror!void {}

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

pub const Ring = switch (builtin.os.tag) {
    .dragonfly,
    .freebsd,
    .macos,
    .netbsd,
    .openbsd,
    => @compileError("kqueue backend coming soon"),

    .linux => @import("Uring.zig"),

    else => @compileError("unsupported"),
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
    usermsg,
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
        target: *const Ring,
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
    usermsg,
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
    usermsg: u16,
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
    _ = @import("queue.zig");
    _ = @import("Uring.zig");
}
