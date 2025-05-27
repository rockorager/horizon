const Router = @This();

const std = @import("std");
const horizon = @import("main.zig");

const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

userdata: ?*anyopaque = null,
routes: std.ArrayListUnmanaged(Route) = .empty,
globals: std.ArrayListUnmanaged(horizon.HandleFn) = .empty,

pub const init: Router = .{};

pub fn deinit(self: *Router, gpa: Allocator) void {
    self.routes.deinit(gpa);
}

pub fn handler(self: *Router) horizon.Handler {
    return .{ .ptr = self, .serveFn = serveHttp };
}

pub fn use(self: *Router, gpa: Allocator, middleware: horizon.HandleFn) Allocator.Error!void {
    try self.globals.append(gpa, middleware);
}

pub fn serveHttp(ctx: *horizon.Context) anyerror!void {
    const self: *Router = @ptrCast(@alignCast(ctx.userdata));
    ctx.userdata = self.userdata;

    const method = ctx.request.method();
    const path = ctx.request.path();
    for (self.routes.items) |route| {
        if (route.method == method and route.match(path)) {
            ctx.pattern = route.pattern;
            const handlers = try ctx.arena.alloc(
                horizon.HandleFn,
                self.globals.items.len + route.handlers.len,
            );
            @memcpy(handlers[0..self.globals.items.len], self.globals.items);
            @memcpy(handlers[self.globals.items.len..], route.handlers);
            ctx.handlers = handlers;
            return ctx.next();
        }
    }

    const handlers = try ctx.arena.alloc(horizon.HandleFn, self.globals.items.len + 1);
    @memcpy(handlers[0..self.globals.items.len], self.globals.items);
    handlers[self.globals.items.len] = horizon.notFound;
    ctx.handlers = handlers;
    return ctx.next();
}

pub fn get(
    self: *Router,
    gpa: Allocator,
    pattern: []const u8,
    handlers: []const horizon.HandleFn,
) Allocator.Error!void {
    return self.addRoute(gpa, .GET, pattern, handlers);
}

pub fn post(
    self: *Router,
    gpa: Allocator,
    pattern: []const u8,
    handlers: []const horizon.HandleFn,
) Allocator.Error!void {
    return self.addRoute(gpa, .POST, pattern, handlers);
}

pub fn addRoute(
    self: *Router,
    gpa: Allocator,
    method: std.http.Method,
    pattern: []const u8,
    handlers: []const horizon.HandleFn,
) Allocator.Error!void {
    try self.routes.append(gpa, .{ .handlers = handlers, .method = method, .pattern = pattern });
    std.sort.pdq(Route, self.routes.items, {}, Route.lessThan);
}

pub const Route = struct {
    handlers: []const horizon.HandleFn,
    method: std.http.Method = .GET,
    pattern: []const u8,

    fn lessThan(_: void, lhs: Route, rhs: Route) bool {
        switch (lhs.compare(rhs)) {
            .eq => return true,
            .lt => return true,
            .gt => return false,
        }
    }

    pub fn match(self: Route, path: []const u8) bool {
        return Route._match(self.pattern, path);
    }

    fn _match(pattern: []const u8, path: []const u8) bool {
        const eqlIgnoreCase = std.ascii.eqlIgnoreCase;
        const startsWith = std.mem.startsWith;

        var route = std.mem.splitScalar(u8, std.mem.trimRight(u8, pattern, "/"), '/');
        var p = std.mem.splitScalar(u8, std.mem.trimRight(u8, path, "/"), '/');

        while (true) {
            const route_segment = route.next() orelse {
                return p.next() == null;
            };
            if (std.mem.eql(u8, route_segment, "*")) return true;
            const path_segment = p.next() orelse return false;

            if (startsWith(u8, route_segment, "{")) continue;

            if (!eqlIgnoreCase(route_segment, path_segment)) return false;
        }
    }

    fn compare(lhs: Route, rhs: Route) std.math.Order {
        var lhs_iter = std.mem.splitScalar(u8, lhs.pattern, '/');
        var rhs_iter = std.mem.splitScalar(u8, rhs.pattern, '/');

        while (true) {
            const lhs_cmp = lhs_iter.next() orelse {
                if (rhs_iter.next() == null) return .eq;
                return .gt;
            };

            const rhs_cmp = rhs_iter.next() orelse return .lt;

            if (lhs_cmp.len == 0 and rhs_cmp.len == 0) continue;

            if (lhs_cmp.len == 0) return .gt;
            if (rhs_cmp.len == 0) return .lt;

            if (lhs_cmp[0] == '{' and rhs_cmp[0] == '{') {
                assert(std.mem.endsWith(u8, lhs_cmp, "}"));
                assert(std.mem.endsWith(u8, rhs_cmp, "}"));
                continue;
            }

            if (std.mem.eql(u8, lhs_cmp, "*")) return .gt;
            if (std.mem.eql(u8, rhs_cmp, "*")) return .lt;

            if (lhs_cmp[0] == '{') {
                assert(std.mem.endsWith(u8, lhs_cmp, "}"));
                return .gt;
            }
            if (rhs_cmp[0] == '{') {
                assert(std.mem.endsWith(u8, rhs_cmp, "}"));
                return .lt;
            }
        }
    }
};

test "Route: lessThan" {
    const expectEqual = std.testing.expectEqual;
    const Order = std.math.Order;
    {
        const lhs: Route = .{ .handlers = undefined, .pattern = "/" };
        const rhs: Route = .{ .handlers = undefined, .pattern = "/" };
        try expectEqual(Order.eq, lhs.compare(rhs));
    }
    {
        const lhs: Route = .{ .handlers = undefined, .pattern = "/" };
        const rhs: Route = .{ .handlers = undefined, .pattern = "/{abc}" };
        try expectEqual(Order.gt, lhs.compare(rhs));
    }

    {
        const lhs: Route = .{ .handlers = undefined, .pattern = "/{abc}/" };
        const rhs: Route = .{ .handlers = undefined, .pattern = "/{abc}" };
        try expectEqual(Order.lt, lhs.compare(rhs));
        try expectEqual(Order.gt, rhs.compare(lhs));
    }

    {
        const lhs: Route = .{ .handlers = undefined, .pattern = "/a/{b}/c" };
        const rhs: Route = .{ .handlers = undefined, .pattern = "/a/b/{c}" };
        try expectEqual(Order.gt, lhs.compare(rhs));
        try expectEqual(Order.lt, rhs.compare(lhs));
    }

    {
        const lhs: Route = .{ .handlers = undefined, .pattern = "/a/b" };
        const rhs: Route = .{ .handlers = undefined, .pattern = "/a/{b}" };
        try expectEqual(Order.lt, lhs.compare(rhs));
        try expectEqual(Order.gt, rhs.compare(lhs));
    }

    {
        const lhs: Route = .{ .handlers = undefined, .pattern = "/a/*" };
        const rhs: Route = .{ .handlers = undefined, .pattern = "/a/{b}" };
        try expectEqual(Order.gt, lhs.compare(rhs));
        try expectEqual(Order.lt, rhs.compare(lhs));
    }

    {
        const lhs: Route = .{ .handlers = undefined, .pattern = "/a/{b}/c" };
        const rhs: Route = .{ .handlers = undefined, .pattern = "/a/{b}/{c}" };
        try expectEqual(Order.lt, lhs.compare(rhs));
        try expectEqual(Order.gt, rhs.compare(lhs));
    }
}

test "Route: match" {
    const expect = std.testing.expect;

    try expect(Route._match("/", "/"));
    try expect(!Route._match("/", "/index.html"));
    try expect(Route._match("/{p}", "/index.html"));
    try expect(Route._match("/root/{p}", "/root/index.html"));
    try expect(Route._match("/root/{p}/abc", "/root/foo/abc"));
    try expect(!Route._match("/root/{p}/abc", "/root/foo/foo"));
    try expect(Route._match("/*", "/root/foo/foo"));
    try expect(Route._match("/*", "/"));
}
