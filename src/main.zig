const std = @import("std");
const builtin = @import("builtin");

const log = @import("log.zig");
const hz = @import("horizon.zig");

pub fn main() !void {
    log.init(.debug);
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
    var s: hz.Server = undefined;
    try s.init(gpa, .{});

    try s.run(gpa);
}
