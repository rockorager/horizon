const Uring = @This();

const std = @import("std");

const io = @import("io.zig");

const linux = std.os.linux;
const posix = std.posix;

const common_flags: u32 =
    linux.IORING_SETUP_SUBMIT_ALL | // Keep submitting events even if one had an error
    linux.IORING_SETUP_CLAMP | // Clamp entries to system supported max
    linux.IORING_SETUP_DEFER_TASKRUN | // Defer work until we submit tasks
    linux.IORING_SETUP_COOP_TASKRUN | // Don't interupt userspace when task is complete
    linux.IORING_SETUP_SINGLE_ISSUER; // Only a single thread will issue tasks

ring: linux.IoUring,

/// Initialize a Ring
pub fn init(entries: u16) !Uring {
    var params = std.mem.zeroInit(linux.io_uring_params, .{
        .flags = common_flags,
        .sq_thread_idle = 1000,
    });

    return .{
        .ring = try .init_params(entries, &params),
    };
}

/// Initializes a child Ring which can be woken up by self. This must be called from the thread
/// which will operate the child ring. Initializes with the same queue size as the parent
pub fn initChild(self: Uring) !Uring {
    const flags: u32 = common_flags | linux.IORING_SETUP_ATTACH_WQ;

    var params = std.mem.zeroInit(linux.io_uring_params, .{
        .flags = flags,
        .sq_thread_idle = 1000,
        .wq_fd = @as(u32, @bitCast(self.ring.fd)),
    });

    const entries: u16 = @intCast(self.ring.sq.sqes.len);

    return .{
        .ring = try .init_params(entries, &params),
    };
}

/// Submit all queued entries
pub fn submitAndWait(self: *Uring) !void {
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

/// Blocks until a completion is available
pub fn nextCompletion(self: *Uring) ?io.Completion {
    const ready = self.ring.cq_ready();
    if (ready == 0) return null;
    const head = self.ring.cq.head.* & self.ring.cq.mask;
    const cqe = self.ring.cq.cqes[head];
    self.ring.cq_advance(1);

    return .{
        .userdata = cqe.user_data,
        .result = cqe.res,
        .flags = @bitCast(cqe.flags),
    };
}

/// Multishot accept
pub fn accept(self: *Uring, fd: posix.fd_t, userdata: *anyopaque) error{SubmissionQueueFull}!void {
    const sqe = try self.getSqe();
    sqe.prep_multishot_accept(fd, null, null, 0);
    sqe.user_data = @intFromPtr(userdata);
}

pub fn msgRing(
    self: *Uring,
    target: io.Ring,
    /// The result that will be placed onto the target Ring
    result: u16,
    /// The userdata field that will be populated on the target Ring
    target_userdata: *anyopaque,
    /// The userdata associated with the operation to send the message
    userdata: *anyopaque,
) error{SubmissionQueueFull}!void {
    const sqe = try self.getSqe();
    sqe.prep_rw(.MSG_RING, target.ring.fd, 0, result, @intFromPtr(target_userdata));
    sqe.user_data = @intFromPtr(userdata);
    sqe.flags |= linux.IOSQE_CQE_SKIP_SUCCESS;
}

pub fn recv(self: *Uring, fd: posix.fd_t, buffer: []u8, userdata: *anyopaque) error{SubmissionQueueFull}!void {
    const sqe = try self.getSqe();
    sqe.prep_recv(fd, buffer, 0);
    sqe.user_data = @intFromPtr(userdata);
}

pub fn cancel(
    self: *Uring,
    /// The userdata field to filter for. The first submission with this value will be cancelled
    cancel_userdata: *anyopaque,
    /// The userdata associated with the cancel submission
    userdata: *anyopaque,
) error{SubmissionQueueFull}!void {
    const sqe = try self.getSqe();
    sqe.prep_cancel(@intFromPtr(cancel_userdata), 0);
    sqe.user_data = @intFromPtr(userdata);
}

pub fn close(self: *Uring, fd: posix.fd_t, userdata: *anyopaque) error{SubmissionQueueFull}!void {
    const sqe = try self.getSqe();
    sqe.prep_close(fd);
    sqe.user_data = @intFromPtr(userdata);
}

pub fn write(
    self: *Uring,
    fd: posix.fd_t,
    buffer: []const u8,
    userdata: *anyopaque,
) error{SubmissionQueueFull}!void {
    const sqe = try self.getSqe();
    sqe.prep_write(fd, buffer, 0);
    sqe.user_data = @intFromPtr(userdata);
}

pub fn writev(
    self: *Uring,
    fd: posix.fd_t,
    iovecs: []const posix.iovec_const,
    userdata: *anyopaque,
) error{SubmissionQueueFull}!void {
    const sqe = try self.getSqe();
    sqe.prep_writev(fd, iovecs, 0);
    sqe.user_data = @intFromPtr(userdata);
}

/// Gets an sqe from the ring. If one isn't available, a submit occurs and we retry
fn getSqe(self: *Uring) error{SubmissionQueueFull}!*linux.io_uring_sqe {
    return self.ring.get_sqe() catch {
        _ = self.ring.submit() catch 0;
        return self.ring.get_sqe();
    };
}
