const std = @import("std");
const builtin = @import("builtin");

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

pub const SubmissionQueueEntry = extern struct {
    opcode: Op,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: u64,
    addr: u64,
    len: u32,
    rw_flags: u32,
    user_data: u64,
    buf_index: u16,
    personality: u16,
    splice_fd_in: i32,
    addr3: u64,
    resv: u64,
};

pub const Completion = extern struct {
    userdata: u64,
    result: i32,
    flags: Flags,

    pub const Flags = packed struct(u32) {
        /// If set, the upper 16 bits are the buffer ID
        buffer: bool = false,

        /// If set, the parent SQE will generate more CQEs
        has_more: bool = false,

        /// If set, more data is available in the socket recv
        socket_nonempty: bool = false,

        /// Set for notification CQEs. Can be used to distinct them from sends for zerocopy
        notification: bool = false,

        /// The buffer was partially consumed and will get more completions.
        buffer_more: bool = false,

        _padding: u11 = 0,

        buffer_id: u16 = 0,
    };

    pub const Error = error{
        Canceled,
        ConnectionResetByPeer,
        Unexpected,
    };

    pub fn err(self: Completion) std.posix.E {
        if (self.result > -4096 and self.result < 0) {
            return @as(std.posix.E, @enumFromInt(-self.result));
        }
        return .SUCCESS;
    }

    pub fn unwrap(self: Completion) Completion.Error!u16 {
        switch (self.err()) {
            .SUCCESS => return @intCast(self.result),
            .CONNRESET => return error.ConnectionResetByPeer,
            .CANCELED => return error.Canceled,
            else => {
                std.log.err("completion error: {}", .{self.err()});
                return error.Unexpected;
            },
        }
    }
};

pub const Op = enum(u8) {
    NOP,
    READV,
    WRITEV,
    FSYNC,
    READ_FIXED,
    WRITE_FIXED,
    POLL_ADD,
    POLL_REMOVE,
    SYNC_FILE_RANGE,
    SENDMSG,
    RECVMSG,
    TIMEOUT,
    TIMEOUT_REMOVE,
    ACCEPT,
    ASYNC_CANCEL,
    LINK_TIMEOUT,
    CONNECT,
    FALLOCATE,
    OPENAT,
    CLOSE,
    FILES_UPDATE,
    STATX,
    READ,
    WRITE,
    FADVISE,
    MADVISE,
    SEND,
    RECV,
    OPENAT2,
    EPOLL_CTL,
    SPLICE,
    PROVIDE_BUFFERS,
    REMOVE_BUFFERS,
    TEE,
    SHUTDOWN,
    RENAMEAT,
    UNLINKAT,
    MKDIRAT,
    SYMLINKAT,
    LINKAT,
    MSG_RING,
    FSETXATTR,
    SETXATTR,
    FGETXATTR,
    GETXATTR,
    SOCKET,
    URING_CMD,
    SEND_ZC,
    SENDMSG_ZC,
    READ_MULTISHOT,
    WAITID,
    FUTEX_WAIT,
    FUTEX_WAKE,
    FUTEX_WAITV,
    FIXED_FD_INSTALL,
    FTRUNCATE,

    _,
};

test Ring {
    // Ensure the exposed API exists for all backends
    try std.testing.expect(std.meta.hasMethod(Ring, "init"));
    try std.testing.expect(std.meta.hasMethod(Ring, "deinit"));
    try std.testing.expect(std.meta.hasMethod(Ring, "submitAndWait"));
    try std.testing.expect(std.meta.hasMethod(Ring, "nextCompletion"));
    try std.testing.expect(std.meta.hasMethod(Ring, "accept"));
    try std.testing.expect(std.meta.hasMethod(Ring, "msgRing"));
    try std.testing.expect(std.meta.hasMethod(Ring, "cancel"));
    try std.testing.expect(std.meta.hasMethod(Ring, "close"));
    try std.testing.expect(std.meta.hasMethod(Ring, "recv"));
    try std.testing.expect(std.meta.hasMethod(Ring, "write"));
    try std.testing.expect(std.meta.hasMethod(Ring, "writev"));
}
