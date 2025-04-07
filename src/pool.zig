const std = @import("std");

const Allocator = std.mem.Allocator;

pub fn MemoryPool(comptime T: type) type {
    return struct {
        const Self = @This();

        const Node = struct {
            next: ?*Node = null,
            item: T,
        };

        /// Linked list of nodes
        free_list: ?*Node = null,

        /// We store the count so that we don't have to follow every pointer to get it. Cache
        /// destruction avoided
        count: usize = 0,

        pub const empty: Self = .{};

        pub fn deinit(self: *Self, gpa: Allocator) void {
            self.shrink(gpa, 0);
            self.* = undefined;
        }

        pub fn create(self: *Self, gpa: Allocator) Allocator.Error!*T {
            if (self.free_list) |node| {
                self.free_list = node.next;
                self.count -= 1;
                return &node.item;
            }

            const node = try gpa.create(Node);
            node.* = .{ .next = null, .item = undefined };
            return &node.item;
        }

        pub fn destroy(self: *Self, ptr: *T) void {
            ptr.* = undefined;

            const node: *Node = @alignCast(@fieldParentPtr("item", ptr));
            node.next = self.free_list;
            self.free_list = node;
            self.count += 1;
        }

        /// Shrinks the pool to be no more than size unused items
        pub fn shrink(self: *Self, gpa: Allocator, size: usize) void {
            while (self.count > size) : (self.count -= 1) {
                const node = self.free_list.?;
                self.free_list = node.next;
                gpa.destroy(node);
            }
        }
    };
}

test "memory pool" {
    var pool: MemoryPool(u8) = .empty;
    defer pool.deinit(std.testing.allocator);

    const a = try pool.create(std.testing.allocator);
    const b = try pool.create(std.testing.allocator);
    const c = try pool.create(std.testing.allocator);

    try std.testing.expectEqual(0, pool.count);

    pool.destroy(a);
    pool.destroy(b);
    pool.destroy(c);

    try std.testing.expectEqual(3, pool.count);

    pool.shrink(std.testing.allocator, 2);
    try std.testing.expectEqual(2, pool.count);
    pool.shrink(std.testing.allocator, 0);
    try std.testing.expectEqual(0, pool.count);
    try std.testing.expect(pool.free_list == null);
}
