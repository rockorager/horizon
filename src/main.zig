const std = @import("std");
const io = @import("ourio");

const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const http = std.http;
const posix = std.posix;

pub const Router = @import("Router.zig");
pub const Server = @import("Server.zig");

pub const Handler = struct {
    ptr: ?*anyopaque = null,
    serveFn: HandleFn,

    /// Initialize a handler from a Type which has a serveHttp method
    pub fn init(comptime T: type, ptr: *T) Handler {
        return .{ .ptr = ptr, .serveFn = T.serveHttp };
    }

    pub fn serveHttp(self: Handler, ctx: *Context) anyerror!void {
        ctx.userdata = self.ptr;
        return self.serveFn(ctx);
    }
};

pub const HandleFn = *const fn (*Context) anyerror!void;

pub const Context = struct {
    /// Arena allocator which will be freed once the response has been written
    arena: Allocator,

    /// Deadline for the response, in seconds from unix epoch
    deadline: u64 = 0,

    /// The io.Ring for this thread. Users may schedule additional tasks to be completed as part of
    /// the Request process. The lifetime of context, request, and responsewriter are all tied to
    /// the underlying Connection, meaning users may store a these and perform additional async
    /// tasks before calling ctx.sendResponse. Eg, fetching external data from an API, performing a
    /// DB query asynchronously, etc
    io: *io.Ring,

    userdata: ?*anyopaque = null,
    pattern: []const u8 = "",
    handlers: []const HandleFn = &.{},
    idx: usize = 0,
    request: Request = .{},
    response: Response,
    kv: std.StringHashMapUnmanaged(Value) = .empty,
    direction: enum { wind, unwind } = .wind,

    pub const Value = union(enum) {
        int: i128,
        string: []const u8,
        list: []const []const u8,
        bool: bool,
        @"enum": u128,
        pointer: ?*anyopaque,
    };

    pub fn put(self: *Context, key: []const u8, value: Value) Allocator.Error!void {
        return self.kv.put(self.arena, key, value);
    }

    pub fn get(self: *Context, key: []const u8) ?Value {
        return self.kv.get(key);
    }

    pub fn next(self: *Context) anyerror!void {
        switch (self.direction) {
            .wind => {
                const handler = self.handlers[self.idx];

                if (self.idx == self.handlers.len - 1) {
                    self.direction = .unwind;
                } else {
                    self.idx += 1;
                }

                return handler(self);
            },

            .unwind => {
                if (self.idx == 0) {
                    const conn: *Server.Connection = @alignCast(@fieldParentPtr("ctx", self));
                    try conn.prepareResponse();
                    try conn.sendResponse();
                    return;
                }
                self.idx -= 1;
                const handler = self.handlers[self.idx];
                return handler(self);
            },
        }
    }

    pub fn expired(self: Context) bool {
        if (self.deadline == 0) return false;
        return std.time.timestamp() > self.deadline;
    }

    pub fn readBody(self: *Context) !void {
        const conn: *Server.Connection = @alignCast(@fieldParentPtr("ctx", self));
        assert(conn.request.body() == null);
        try conn.readBody();
    }

    pub fn param(self: Context, key: []const u8) ?[]const u8 {
        return extractParam(self.pattern, self.request.path(), key);
    }
};

fn extractParam(pattern: []const u8, path: []const u8, key: []const u8) ?[]const u8 {
    if (key.len == 0) return null;
    var iter = std.mem.splitScalar(u8, pattern, '/');
    var path_iter = std.mem.splitScalar(u8, path, '/');
    while (iter.next()) |segment| {
        const val = path_iter.next() orelse return null;
        if (!std.mem.startsWith(u8, segment, "{")) continue;

        if (segment.len < 2) continue;
        const p = segment[1 .. segment.len - 1];
        if (std.ascii.eqlIgnoreCase(p, key)) return val;
    }

    return null;
}

pub const Request = struct {
    /// The raw bytes of the request. This may not be the full request - handlers are called when we
    /// have received the headers. At that point, callers can instruct the response to read the full
    /// body. In this case, the callback will be called again when the full body has been read
    bytes: std.ArrayListUnmanaged(u8) = .empty,

    // add the slice to the internal buffer
    pub fn appendSlice(self: *Request, gpa: Allocator, bytes: []const u8) !void {
        try self.bytes.appendSlice(gpa, bytes);
    }

    pub fn headLen(self: Request) ?usize {
        const idx = std.mem.indexOf(
            u8,
            self.bytes.items,
            "\r\n" ++ "\r\n",
        ) orelse return null;

        return idx + 4;
    }

    /// Returns true if we have received the full header
    pub fn receivedHeader(self: Request) bool {
        return self.headLen() != null;
    }

    /// Returns the body of the request. Null indicates there is a body and we haven't read the
    /// entirety of it. An empty string indicates the request has no body and is not expecting one
    pub fn body(self: Request) ?[]const u8 {
        const head_len = self.headLen() orelse return null;

        const cl = self.contentLength() orelse {
            // TODO: we need to also check for chunked transfer encoding
            return "";
        };

        if (cl + head_len == self.bytes.items.len) return self.bytes.items[head_len..];

        return null;
    }

    /// iterates over headers and trailers
    pub fn headerIterator(self: Request) HeaderIterator {
        assert(self.receivedHeader());
        return .init(self.bytes.items);
    }

    pub fn getHeader(self: Request, key: []const u8) ?[]const u8 {
        var iter = self.headerIterator();
        while (iter.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, key)) return header.value;
        }
        return null;
    }

    /// Returns the HTTP method of this request
    pub fn method(self: Request) http.Method {
        assert(self.receivedHeader());
        // GET / HTTP/1.1
        const idx = std.mem.indexOf(u8, self.bytes.items, "\r\n") orelse unreachable;
        const line = self.bytes.items[0..idx];
        const space = std.mem.indexOfScalar(u8, line, ' ') orelse @panic("TODO: bad request");
        const val = http.Method.parse(line[0..space]);
        return @enumFromInt(val);
    }

    pub fn contentLength(self: Request) ?u64 {
        const value = self.getHeader("Content-Length") orelse return null;
        return std.fmt.parseUnsigned(u64, value, 10) catch @panic("TODO: bad content length");
    }

    pub fn host(self: Request) []const u8 {
        return self.getHeader("Host") orelse @panic("TODO: no host header");
    }

    pub fn keepAlive(self: Request) bool {
        const value = self.getHeader("Connection") orelse return true;
        // fast and slow paths for case matching
        return std.ascii.eqlIgnoreCase(value, "keep-alive");
    }

    pub fn path(self: Request) []const u8 {
        assert(self.receivedHeader());
        const idx = std.mem.indexOf(u8, self.bytes.items, "\r\n") orelse unreachable;
        const line = self.bytes.items[0..idx];
        var iter = std.mem.splitScalar(u8, line, ' ');
        _ = iter.first();
        return iter.next() orelse unreachable;
    }

    /// Validates a request
    pub fn isValid(self: Request, w: *Response) !bool {
        _ = w;
        const m = self.method();
        if (m.requestHasBody()) {
            // We require a content length
            if (self.contentLength() == null) {
                // try errorResponse(w, .bad_request, "Content-Length is required", .{});
                return false;
            }
        }
        return true;
    }
};

pub const Response = struct {
    arena: Allocator,

    body: Body = .{ .dynamic = .empty },

    headers: std.StringHashMapUnmanaged([]const u8) = .{},

    status: ?http.Status = null,

    pub const Body = union(enum) {
        file: struct {
            fd: posix.fd_t,
            size: usize,
        },
        static: []const u8,
        dynamic: std.ArrayListUnmanaged(u8),

        pub fn len(self: Body) usize {
            return switch (self) {
                .file => |f| f.size,
                .static => |s| s.len,
                .dynamic => |d| d.items.len,
            };
        }
    };

    pub fn setHeader(self: *Response, k: []const u8, maybe_v: ?[]const u8) Allocator.Error!void {
        if (maybe_v) |v| {
            return self.headers.put(self.arena, k, v);
        }

        _ = self.headers.remove(k);
    }

    pub fn setStatus(self: *Response, status: http.Status) void {
        self.status = status;
    }

    pub fn typeErasedWrite(ptr: *const anyopaque, bytes: []const u8) anyerror!usize {
        const self: *Response = @constCast(@ptrCast(@alignCast(ptr)));
        switch (self.body) {
            .file => |file| {
                // If we had set a file, we close the descriptor here
                posix.close(file.fd);
                self.body = .{ .dynamic = .empty };
            },
            .static => self.body = .{ .dynamic = .empty },
            .dynamic => {},
        }
        try self.body.dynamic.appendSlice(self.arena, bytes);
        return bytes.len;
    }

    pub fn flush(self: *Response) anyerror!void {
        const ctx: *Context = @alignCast(@fieldParentPtr("response", self));
        ctx.direction = .unwind;
        return ctx.next();
    }

    pub fn any(self: *Response) std.io.AnyWriter {
        return .{ .context = self, .writeFn = Response.typeErasedWrite };
    }
};

pub const HeaderIterator = struct {
    iter: std.mem.SplitIterator(u8, .sequence),

    pub fn init(bytes: []const u8) HeaderIterator {
        var iter = std.mem.splitSequence(u8, bytes, "\r\n");
        // Throw away the first line
        _ = iter.first();
        return .{ .iter = iter };
    }

    pub fn next(self: *HeaderIterator) ?std.http.Header {
        const line = self.iter.next() orelse return null;
        if (line.len == 0) {
            // When we get to the first empty line we are done
            self.iter.index = self.iter.buffer.len;
            return null;
        }
        var kv_iter = std.mem.splitScalar(u8, line, ':');
        const name = kv_iter.first();
        const value = kv_iter.rest();
        return .{
            .name = name,
            .value = std.mem.trim(u8, value, " \t"),
        };
    }
};

pub fn notFound(ctx: *Context) anyerror!void {
    try ctx.response.any().writeAll("404 page not found\n");
    ctx.response.setStatus(.not_found);
    try ctx.response.flush();
}

pub fn errorResponse(
    ctx: *Context,
    status: http.Status,
    comptime format: []const u8,
    args: anytype,
) anyerror!void {
    ctx.response.setStatus(status);
    // Clear the Content-Length header
    try ctx.response.setHeader("Content-Length", null);
    // Set content type
    try ctx.response.setHeader("Content-Type", "text/plain");
    // Print the body
    try ctx.response.any().print(format, args);

    return ctx.response.flush();
}

test {
    _ = @import("Router.zig");
    _ = @import("Server.zig");
    _ = @import("pool.zig");
    _ = @import("sniff.zig");
}

test "extractParam" {
    const expectEqualStrings = std.testing.expectEqualStrings;

    try expectEqualStrings("foo", extractParam("/{bar}", "/foo", "bar").?);
    try expectEqualStrings("foo", extractParam("/root/{bar}", "/root/foo", "bar").?);
    try std.testing.expect(extractParam("/root/{bar}", "/root/foo", "foo") == null);
}
