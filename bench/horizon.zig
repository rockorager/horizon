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

const Foo = struct {
    w: horizon.ResponseWriter,
    count: u8 = 0,
};

const MyHandler = struct {
    fn handler(self: *MyHandler) horizon.Handler {
        // Not much magic here. If your type has a servHttp method, this is a helper to make the
        // itnerface from the type and pointer
        return .init(MyHandler, self);
    }

    pub fn serveHttp(_: *anyopaque, _: *horizon.Context, w: horizon.ResponseWriter, _: horizon.Request) anyerror!void {
        try w.any().print("hello, world", .{});
        try w.flush();
    }
};

const gzip = struct {
    const Handler = struct {
        next: horizon.Handler,

        pub fn init(next: horizon.Handler) Handler {
            return .{ .next = next };
        }

        pub fn handler(self: *Handler) horizon.Handler {
            return .init(Handler, self);
        }

        pub fn serveHttp(
            ptr: *anyopaque,
            ctx: *horizon.Context,
            w: horizon.ResponseWriter,
            r: horizon.Request,
        ) anyerror!void {
            const self: *Handler = @ptrCast(@alignCast(ptr));

            const hdr = r.getHeader("Accept-Encoding") orelse
                return self.next.serveHttp(ctx, w, r);

            if (std.mem.indexOf(u8, hdr, "gzip") == null)
                return self.next.serveHttp(ctx, w, r);

            try w.setHeader("Content-Encoding", "gzip");

            // We need to allocate the writer, it's lifetime could be beyond this function if the
            const gz = try ctx.arena.create(ResponseWriter);
            gz.* = .{ .rw = w };
            try self.next.serveHttp(ctx, gz.responseWriter(), r);
            try gz.flush();
        }
    };

    const ResponseWriter = struct {
        buffer: [4096]u8 = undefined,
        idx: usize = 0,
        rw: horizon.ResponseWriter,

        fn responseWriter(self: *ResponseWriter) horizon.ResponseWriter {
            return .init(ResponseWriter, self);
        }

        pub fn setHeader(ptr: *anyopaque, key: []const u8, value: ?[]const u8) std.mem.Allocator.Error!void {
            const self: *ResponseWriter = @ptrCast(@alignCast(ptr));
            return self.rw.setHeader(key, value);
        }

        pub fn setStatus(ptr: *anyopaque, status: std.http.Status) void {
            const self: *ResponseWriter = @ptrCast(@alignCast(ptr));
            self.rw.setStatus(status);
        }

        pub fn write(ptr: *const anyopaque, bytes: []const u8) anyerror!usize {
            const self: *ResponseWriter = @constCast(@ptrCast(@alignCast(ptr)));

            // If this write is longer than our buffer, we flush then directly compress the bytes
            if (bytes.len > self.buffer.len) {
                try self.flush();
                var fbs = std.io.fixedBufferStream(bytes);
                try std.compress.gzip.compress(fbs.reader(), self.rw.any(), .{});
                return bytes.len;
            }

            // If we have room for the full amount, we memcpy and return
            if (self.idx + bytes.len <= self.buffer.len) {
                @memcpy(self.buffer[self.idx..][0..bytes.len], bytes);
                self.idx += bytes.len;
                return bytes.len;
            }

            const writeable_len = self.buffer.len - self.idx;
            @memcpy(self.buffer[self.idx..][0..writeable_len], bytes[0..writeable_len]);
            self.idx += writeable_len;
            try self.flush();
            const remainder = bytes.len - writeable_len;
            @memcpy(self.buffer[0..remainder], bytes[remainder..]);
            self.idx += remainder;
            return bytes.len;
        }

        fn flush(self: *ResponseWriter) anyerror!void {
            if (self.idx == 0) return;

            var fbs = std.io.fixedBufferStream(self.buffer[0..self.idx]);
            try std.compress.gzip.compress(fbs.reader(), self.rw.any(), .{});
            self.idx = 0;

            return self.rw.flush();
        }
    };
};
