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

    var server: horizon.Server = undefined;
    try server.init(gpa, .{});
    defer server.deinit(gpa);

    var router: horizon.Router = .{};
    defer router.deinit(gpa);

    try router.use(gpa, requestLogger);

    try router.get(gpa, "/*", &.{handleRoot});

    try server.listenAndServe(&io, router.handler());
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
    const rel = if (std.mem.eql(u8, "/", ctx.request.path())) "/index.html" else ctx.request.path();
    const path = try std.fmt.allocPrintZ(ctx.arena, "/home/tim/repos/github.com/rockorager/rockorager.dev/public{s}", .{rel});
    return ctx.serveFile(path);
}
