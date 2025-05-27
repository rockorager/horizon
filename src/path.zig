const std = @import("std");

const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const fs = std.fs;

pub const ResolveError = error{
    InvalidPath,
} || Allocator.Error;

pub fn resolve(gpa: Allocator, root: []const u8, path: []const u8) ResolveError![:0]const u8 {
    assert(std.fs.path.isAbsolute(root));

    var buf: [fs.max_path_bytes]u8 = undefined;
    var fba: std.heap.FixedBufferAllocator = .init(&buf);
    const joined = try fs.path.join(fba.allocator(), &.{ root, path });

    const resolved = try fs.path.resolve(gpa, &.{joined});

    if (!std.mem.startsWith(u8, resolved, root)) return error.InvalidPath;

    return gpa.dupeZ(u8, resolved);
}

test "resolve" {
    const expectEqualStrings = std.testing.expectEqualStrings;
    const expectError = std.testing.expectError;

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const gpa = arena.allocator();

    try expectEqualStrings("/a/b/c", try resolve(gpa, "/", "a/b/c"));
    try expectEqualStrings("/a/b/c", try resolve(gpa, "/a", "/b/c/"));

    try expectError(error.InvalidPath, resolve(gpa, "/a", "../../b/c/"));
}
