const std = @import("std");
const builtin = @import("builtin");
const horizon = @import("horizon");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const gpa, const is_debug = gpa: {
        break :gpa switch (builtin.mode) {
            .Debug, .ReleaseSafe => .{ debug_allocator.allocator(), true },
            .ReleaseFast, .ReleaseSmall => .{ std.heap.smp_allocator, false },
        };
    };
    defer if (is_debug) {
        _ = debug_allocator.deinit();
    };

    var s: horizon.Server = undefined;
    try s.init(gpa, .{ .shutdown_signal = std.posix.SIG.INT });
    defer s.deinit(gpa);

    var my_handler: MyHandler = .{};

    std.log.debug("listening at {}", .{s.addr});

    try s.serve(gpa, my_handler.handler());
}

const MyHandler = struct {
    fn handler(self: *MyHandler) horizon.Handler {
        // Not much magic here. If your type has a servHttp method, this is a helper to make the
        // interface from the type and pointer
        return .init(MyHandler, self);
    }

    pub fn serveHttp(
        _: *anyopaque,
        _: *horizon.Context,
        w: horizon.ResponseWriter,
        _: horizon.Request,
    ) anyerror!void {
        try w.any().print("Hello, world", .{});
    }
};
