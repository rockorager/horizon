const std = @import("std");
const builtin = @import("builtin");

const posix = std.posix;

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

pub const Runtime = switch (builtin.os.tag) {
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
    socket,
    connect,

    userfd,
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
        target: *const Runtime,
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
    _ = @import("net.zig");
}
