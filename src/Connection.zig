const Connection = @This();

const std = @import("std");
const horizon = @import("main.zig");
const ourio = @import("ourio");
const sniff = @import("sniff.zig");

const Allocator = std.mem.Allocator;
const Server = horizon.Server;
const assert = std.debug.assert;
const posix = std.posix;

const log = std.log.scoped(.horizon);

arena: std.heap.ArenaAllocator,

server: *Server,
buf: [4096]u8 = undefined,
fd: posix.socket_t,
write_buf: std.ArrayListUnmanaged(u8) = .empty,
vecs: [2]posix.iovec_const = undefined,
pipe: ?[2]posix.fd_t = null,
written: usize = 0,

deadline: i64,

ctx: horizon.Context,

state: State = .init,

recv_task: ?*ourio.Task = null,

const State = enum {
    init,
    reading_headers,
    handling_request,
    reading_body,
    waiting_send_response,
    idle,
    close,
    waiting_for_destruction,
};

const WriteState = enum {
    headers_only,
    headers_and_body,
    body_only,
};

pub fn init(
    self: *Connection,
    gpa: Allocator,
    io: *ourio.Ring,
    server: *Server,
    fd: posix.socket_t,
) !void {
    self.* = .{
        .arena = .init(gpa),
        .server = server,
        .fd = fd,
        .ctx = undefined,
        .deadline = std.time.timestamp() + server.timeout.read_header,
    };

    self.ctx = .{
        .arena = self.arena.allocator(),
        .deadline = 0,
        .io = io,
        .request = .{},
        .response = .{ .arena = self.arena.allocator() },
    };

    self.recv_task = try io.recv(
        fd,
        &self.buf,
        .{
            .ptr = self,
            .msg = @intFromEnum(Connection.Msg.reading_headers),
            .cb = Connection.handleMsg,
        },
    );
    if (server.timeout.read_header > 0) {
        try self.recv_task.?.setDeadline(io, .{ .sec = self.deadline });
    }
    self.state = .reading_headers;
}

fn deinit(self: *Connection) void {
    self.arena.deinit();
    self.* = undefined;
}

const Msg = enum {
    reading_headers,
    reading_body,
    write_response,
    sendfile_lhs,
    sendfile_rhs,
    close,
    destroy,
};

fn handleMsg(io: *ourio.Ring, task: ourio.Task) anyerror!void {
    const self = task.userdataCast(Connection);
    const result = task.result.?;
    state: switch (task.msgToEnum(Connection.Msg)) {
        .reading_headers => {
            assert(result == .recv);

            const n = result.recv catch continue :state .close;
            if (n == 0) continue :state .close;

            try self.ctx.request.appendSlice(self.arena.allocator(), self.buf[0..n]);

            switch (self.ctx.request.receivedHeader()) {
                // When we receive the full header, we pass it to the handler
                true => {
                    if (self.ctx.request.isValid()) {
                        try self.server.handler.serveHttp(&self.ctx);
                    } else |_| {
                        try horizon.errorResponse(&self.ctx, .bad_request, "Bad request", .{});
                    }
                },

                false => {
                    // We haven't received a full HEAD. prep another recv
                    const new_task = try io.recv(self.fd, &self.buf, .{
                        .ptr = self,
                        .msg = @intFromEnum(Connection.Msg.reading_headers),
                        .cb = Connection.handleMsg,
                    });
                    if (self.server.timeout.read_header > 0) {
                        try new_task.setDeadline(io, .{ .sec = self.deadline });
                    }
                },
            }
        },

        .reading_body => {
            assert(result == .recv);

            const n = result.recv catch continue :state .close;
            if (n == 0) continue :state .close;

            try self.ctx.request.appendSlice(self.arena.allocator(), self.buf[0..n]);

            try self.readBody();
        },

        .write_response => {
            assert(result == .write or result == .writev);
            const n = switch (result) {
                .write => result.write catch {
                    continue :state .close;
                },
                .writev => result.writev catch {
                    continue :state .close;
                },
                else => unreachable,
            };

            self.written += n;
            if (!self.responseComplete()) {
                return self.sendResponse();
            }

            // If the worker is quitting, we can close this connection
            if (!self.ctx.request.keepAlive()) {
                continue :state .close;
            }

            // Keep the connection alive
            self.reset();
            const new_task = try io.recv(self.fd, &self.buf, .{
                .ptr = self,
                .msg = @intFromEnum(Connection.Msg.reading_headers),
                .cb = Connection.handleMsg,
            });
            if (self.server.timeout.idle > 0) {
                self.deadline = std.time.timestamp() + self.server.timeout.idle;
                try new_task.setDeadline(io, .{ .sec = self.deadline });
            }
        },

        .sendfile_lhs => {
            const n = result.splice catch {
                continue :state .close;
            };

            const file = self.ctx.response.body.file;

            if (n < task.req.splice.nbytes) {
                const w = self.pipe.?[1];

                const new_offset = n + task.req.splice.offset;
                const new_nbytes = task.req.splice.nbytes - new_offset;

                const null_offset: i64 = -1;
                const lhs = try self.server.io.splice(
                    file.fd,
                    new_offset,
                    w,
                    @bitCast(null_offset),
                    new_nbytes,
                    .{
                        .ptr = self,
                        .msg = @intFromEnum(Connection.Msg.sendfile_lhs),
                        .cb = Connection.handleMsg,
                    },
                );

                if (self.server.timeout.write > 0) {
                    try lhs.setDeadline(self.server.io, .{ .sec = self.deadline });
                }

                return;
            }

            _ = try io.close(file.fd, .{});
        },

        .sendfile_rhs => {
            const n = result.splice catch {
                continue :state .close;
            };

            if (n < task.req.splice.nbytes) {
                const r = self.pipe.?[0];

                const new_nbytes = task.req.splice.nbytes - n;

                const null_offset: i64 = -1;
                const rhs = try self.server.io.splice(
                    r,
                    @bitCast(null_offset),
                    self.fd,
                    @bitCast(null_offset),
                    new_nbytes,
                    .{
                        .ptr = self,
                        .msg = @intFromEnum(Connection.Msg.sendfile_rhs),
                        .cb = Connection.handleMsg,
                    },
                );

                if (self.server.timeout.write > 0) {
                    try rhs.setDeadline(self.server.io, .{ .sec = self.deadline });
                }

                return;
            }

            // If the worker is quitting, we can close this connection
            if (!self.ctx.request.keepAlive()) {
                continue :state .close;
            }

            // Keep the connection alive
            self.reset();
            const new_task = try io.recv(self.fd, &self.buf, .{
                .ptr = self,
                .msg = @intFromEnum(Connection.Msg.reading_headers),
                .cb = Connection.handleMsg,
            });
            if (self.server.timeout.idle > 0) {
                self.deadline = std.time.timestamp() + self.server.timeout.idle;
                try new_task.setDeadline(io, .{ .sec = self.deadline });
            }
        },

        .close => {
            if (self.pipe) |pipe| {
                _ = try io.close(pipe[0], .{});
                _ = try io.close(pipe[1], .{});
                self.pipe = null;
            }
            _ = try io.close(self.fd, .{
                .ptr = self,
                .msg = @intFromEnum(Connection.Msg.destroy),
                .cb = Connection.handleMsg,
            });
        },

        .destroy => {
            assert(result == .close);
            _ = result.close catch |err| {
                log.err("close error: {}", .{err});
            };
            const server = self.server;
            self.deinit();

            server.pool.destroy(self);
        },
    }
}

/// Writes the header into write_buf
pub fn prepareHeader(self: *Connection) !void {
    var headers = &self.write_buf;
    const resp = self.ctx.response;
    const status = resp.status orelse .ok;

    // Base amount to cover start line, content-length, content-type, and trailing \r\n
    var len: usize = 128;
    {
        var iter = resp.headers.iterator();
        while (iter.next()) |entry| {
            len += entry.key_ptr.len + entry.value_ptr.len + 2;
        }
    }

    try headers.ensureTotalCapacity(self.arena.allocator(), len);
    var writer = headers.fixedWriter();

    if (status.phrase()) |phrase| {
        writer.print("HTTP/1.1 {d} {s}\r\n", .{ @intFromEnum(status), phrase }) catch unreachable;
    } else {
        writer.print("HTTP/1.1 {d}\r\n", .{@intFromEnum(status)}) catch unreachable;
    }

    if (resp.headers.get("Content-Length") == null) {
        switch (status) {
            .not_modified => {},
            else => {
                writer.print("Content-Length: {d}\r\n", .{resp.body.len()}) catch unreachable;
            },
        }
    }

    if (resp.headers.get("Content-Type") == null) {
        const ct = switch (resp.body) {
            .file => "text/plain", // TODO: fix this
            .static => |s| sniff.detectContentType(s),
            .dynamic => |*d| sniff.detectContentType(d.items),
        };
        writer.print("Content-Type: {s}\r\n", .{ct}) catch unreachable;
    }

    var iter = resp.headers.iterator();
    while (iter.next()) |h| {
        writer.print(
            "{s}: {s}\r\n",
            .{ h.key_ptr.*, h.value_ptr.* },
        ) catch unreachable;
    }

    writer.writeAll("\r\n") catch unreachable;
}

/// Prepares a recv request to read the body of the request. If the body is fully read, the
/// handler is called again
pub fn readBody(self: *Connection) !void {
    const head_len = self.ctx.request.headLen() orelse @panic("TODO");
    const cl = self.ctx.request.contentLength() orelse @panic("TODO");

    if (head_len + cl == self.ctx.request.bytes.items.len) {
        return self.server.handler.serveHttp(&self.ctx);
    }

    self.state = .reading_body;

    _ = try self.server.io.recv(self.fd, &self.buf, .{
        .ptr = self,
        .msg = @intFromEnum(Connection.Msg.reading_body),
        .cb = Connection.handleMsg,
    });
}

pub fn prepareResponse(self: *Connection) !void {
    // Prepare the header
    try self.prepareHeader();

    // Set the deadline
    if (self.server.timeout.write > 0) {
        self.deadline = std.time.timestamp() + self.server.timeout.write;
    }
}

pub fn sendResponse(self: *Connection) !void {
    self.state = .waiting_send_response;

    const headers = self.write_buf.items;
    const body = switch (self.ctx.response.body) {
        .file => "",
        .static => |s| s,
        .dynamic => |*d| d.items,
    };

    const wstate: WriteState =
        if (self.written < headers.len and body.len == 0)
            .headers_only
        else if (self.written < headers.len)
            .headers_and_body
        else
            .body_only;

    const new_task = switch (wstate) {
        .headers_only => try self.server.io.write(self.fd, headers[self.written..], .beginning, .{
            .ptr = self,
            .msg = @intFromEnum(Connection.Msg.write_response),
            .cb = Connection.handleMsg,
        }),

        .headers_and_body => blk: {
            const unwritten = headers[self.written..];
            self.vecs[0] = .{
                .base = unwritten.ptr,
                .len = unwritten.len,
            };
            self.vecs[1] = .{
                .base = body.ptr,
                .len = body.len,
            };

            break :blk try self.server.io.writev(self.fd, &self.vecs, .beginning, .{
                .ptr = self,
                .msg = @intFromEnum(Connection.Msg.write_response),
                .cb = Connection.handleMsg,
            });
        },

        .body_only => blk: {
            if (self.ctx.response.body == .file) {
                return self.sendFile();
            }
            const offset = self.written - headers.len;
            const unwritten_body = body[offset..];
            break :blk try self.server.io.write(self.fd, unwritten_body, .beginning, .{
                .ptr = self,
                .msg = @intFromEnum(Connection.Msg.write_response),
                .cb = Connection.handleMsg,
            });
        },
    };

    if (self.server.timeout.write > 0) {
        try new_task.setDeadline(self.server.io, .{ .sec = self.deadline });
    }
}

fn sendFile(self: *Connection) !void {
    if (self.pipe == null) {
        self.pipe = try posix.pipe2(.{ .CLOEXEC = true });
    }

    const r = self.pipe.?[0];
    const w = self.pipe.?[1];

    const file = self.ctx.response.body.file;

    const null_offset: i64 = -1;
    const lhs = try self.server.io.splice(
        file.fd,
        0,
        w,
        @bitCast(null_offset),
        file.size,
        .{
            .ptr = self,
            .msg = @intFromEnum(Connection.Msg.sendfile_lhs),
            .cb = Connection.handleMsg,
        },
    );
    const rhs = try self.server.io.splice(
        r,
        @bitCast(null_offset),
        self.fd,
        @bitCast(null_offset),
        file.size,
        .{
            .ptr = self,
            .msg = @intFromEnum(Connection.Msg.sendfile_rhs),
            .cb = Connection.handleMsg,
        },
    );

    if (self.server.timeout.write > 0) {
        try lhs.setDeadline(self.server.io, .{ .sec = self.deadline });
        try rhs.setDeadline(self.server.io, .{ .sec = self.deadline });
    }
}

fn reset(self: *Connection) void {
    _ = self.arena.reset(.free_all);
    self.ctx = .{
        .arena = self.arena.allocator(),
        .io = self.server.io,
        .response = .{ .arena = self.arena.allocator() },
    };

    self.write_buf = .empty;
    self.written = 0;
}

fn responseComplete(self: *Connection) bool {
    return self.written == (self.write_buf.items.len + self.ctx.response.body.len());
}
