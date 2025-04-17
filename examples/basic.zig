const std = @import("std");
const builtin = @import("builtin");
const horizon = @import("horizon");
const io = @import("io");

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

    var ring = try io.Runtime.init(gpa, 64);
    defer ring.deinit();

    var server: horizon.Server = undefined;
    try server.init(gpa, .{});
    defer server.deinit(gpa);

    var my_handler: MyHandler = .{};

    try server.listenAndServe(&ring, my_handler.handler());
    std.log.debug("listening at {}", .{server.addr});

    try ring.run(.until_done);
}

const MyHandler = struct {
    fn handler(self: *MyHandler) horizon.Handler {
        // Not much magic here. If your type has a serveHttp method, this is a helper to make the
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
        return w.flush();
    }
};
