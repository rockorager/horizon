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
    try s.init(gpa, .{});

    var gzip_handler: gzip.Handler = .init(.{ .ptr = undefined, .serveFn = serveHttp });

    try s.run(gpa, gzip_handler.handler());
}

pub fn serveHttp(_: *anyopaque, resp: horizon.ResponseWriter, req: horizon.Request) anyerror!void {
    std.log.debug("path={s}", .{req.path()});
    try resp.any().print("hello, world", .{});
}

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
            w: horizon.ResponseWriter,
            r: horizon.Request,
        ) anyerror!void {
            const self: *Handler = @ptrCast(@alignCast(ptr));

            const hdr = r.getHeader("Accept-Encoding") orelse
                return self.next.serveHttp(w, r);

            if (std.mem.indexOf(u8, hdr, "gzip") == null)
                return self.next.serveHttp(w, r);

            var gz: ResponseWriter = .{ .rw = w };
            try self.next.serveHttp(
                gz.responseWriter(),
                r,
            );
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
        }
    };
};
