const std = @import("std");
const io = @import("io");
const posix = std.posix;

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .{};
    var rt: io.Runtime = try .init(gpa.allocator(), 64);
    defer rt.deinit();

    const Foo = struct {
        val: usize = 0,
        fn callback(ptr: ?*anyopaque, _: *io.Runtime, _: u16, result: io.Result) anyerror!void {
            std.log.debug("callback", .{});
            _ = result.poll catch |err| return err;
            const self = io.ptrCast(@This(), ptr);
            self.val += 1;
        }
    };

    var foo: Foo = .{};
    const pipe = try posix.pipe2(.{ .CLOEXEC = true });

    _ = try rt.poll(pipe[0], posix.POLL.IN, &foo, 0, Foo.callback);
    try std.testing.expectEqual(1, rt.workQueueSize());

    _ = try posix.write(pipe[1], "io_uring is better");
    try rt.run(.once);
    try std.testing.expectEqual(1, foo.val);
}
