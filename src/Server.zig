const Server = @This();

const std = @import("std");
const horizon = @import("main.zig");
const ourio = @import("ourio");

const Allocator = std.mem.Allocator;
const Connection = @import("Connection.zig");
const MemoryPool = @import("pool.zig").MemoryPool;
const assert = std.debug.assert;
const net = std.net;
const posix = std.posix;

const log = std.log.scoped(.horizon);

gpa: Allocator,

addr: net.Address,

handler: horizon.Handler,

fd: ?posix.fd_t = null,

io: *ourio.Ring = undefined,

timeout: struct {
    read_header: u8 = 10,
    read_body: u8 = 10,
    idle: u8 = 60,
    write: u8 = 20,
} = .{},

accept_task: ?*ourio.Task = null,

client_addr: net.Address = undefined,
client_addr_len: posix.socklen_t = 0,

pool: MemoryPool(Connection) = .empty,

const Msg = enum(u16) {
    new_connection,
};

pub fn deinit(self: *Server) void {
    if (self.accept_task) |t| _ = t.cancel(self.io, .{}) catch {};
    if (self.fd) |fd| _ = self.io.close(fd, .{}) catch {};
    self.io.backend.submit(&self.io.submission_q) catch {};
    self.pool.deinit(self.gpa);
}

pub fn listenAndServe(self: *Server, io: *ourio.Ring) !void {
    self.io = io;
    const flags = posix.SOCK.STREAM | posix.SOCK.CLOEXEC;

    const fd = try posix.socket(self.addr.any.family, flags, 0);
    errdefer posix.close(fd);

    try posix.setsockopt(
        fd,
        posix.SOL.SOCKET,
        posix.SO.REUSEADDR,
        &std.mem.toBytes(@as(c_int, 1)),
    );
    try posix.setsockopt(
        fd,
        posix.SOL.SOCKET,
        posix.SO.REUSEPORT,
        &std.mem.toBytes(@as(c_int, 1)),
    );

    try posix.bind(fd, &self.addr.any, self.addr.getOsSockLen());

    var sock_len = self.addr.getOsSockLen();
    try posix.getsockname(fd, &self.addr.any, &sock_len);
    try posix.listen(fd, 64);

    self.fd = fd;
    try self.accept(io);
}

fn accept(self: *Server, io: *ourio.Ring) !void {
    assert(self.fd != null);

    self.client_addr_len = posix.sockaddr.SS_MAXSIZE;
    self.accept_task = try io.accept(
        self.fd.?,
        &self.client_addr.any,
        &self.client_addr_len,
        .{
            .ptr = self,
            .msg = @intFromEnum(Msg.new_connection),
            .cb = onCompletion,
        },
    );
}

fn onCompletion(io: *ourio.Ring, task: ourio.Task) anyerror!void {
    const self = task.userdataCast(Server);
    const result = task.result.?;

    switch (task.msgToEnum(Msg)) {
        .new_connection => {
            const fd = result.accept catch |err| {
                switch (err) {
                    error.Canceled => {},
                    else => log.err("accept error: {}", .{err}),
                }
                return;
            };

            try self.accept(io);

            const conn = try self.pool.create(self.gpa);
            try conn.init(self.gpa, io, self, fd);
        },
    }
}
