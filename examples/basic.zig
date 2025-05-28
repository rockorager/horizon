const std = @import("std");
const builtin = @import("builtin");
const horizon = @import("horizon");
const ourio = @import("ourio");

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

    var io = try ourio.Ring.init(gpa, 64);
    defer io.deinit();

    const addr = std.net.Address.parseIp4("127.0.0.1", 8083) catch unreachable;

    var router: horizon.Router = .{};
    defer router.deinit(gpa);

    // try router.use(gpa, requestLogger);

    try router.get(gpa, "/*", &.{handleRoot});

    var server: horizon.Server = .{
        .gpa = gpa,
        .addr = addr,
        .handler = router.handler(),
    };
    // var server: horizon.Server = undefined;
    // try server.init(gpa, .{ .addr = addr, .workers = 2 });
    defer server.deinit();

    try server.listenAndServe(&io);
    std.log.debug("listening at {}", .{server.addr});
    try io.run(.until_done);
}

fn requestLogger(ctx: *horizon.Context) anyerror!void {
    if (ctx.get("request_start_time")) |v| {
        std.log.err("status={} request took {d} microseconds", .{ ctx.response.status.?, std.time.microTimestamp() - @as(i64, @intCast(v.int)) });
    } else {
        try ctx.put("request_start_time", .{ .int = std.time.microTimestamp() });
    }
    return ctx.next();
}

fn handleRoot(ctx: *horizon.Context) anyerror!void {
    return ctx.serveFile("/var/www/rockorager.dev", ctx.request.path());
}
