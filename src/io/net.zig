const std = @import("std");
const io = @import("io.zig");

const posix = std.posix;
const Allocator = std.mem.Allocator;
const Uri = std.Uri;

const assert = std.debug.assert;

pub fn tcpConnectToHost(
    rt: *io.Runtime,
    host: []const u8,
    port: u16,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) std.net.GetAddressListError!*ConnectTask {
    // TODO: getAddressList could be rewritten to be async. It accesses the filesystem and could
    // make a DNS request
    const list = try std.net.getAddressList(rt.gpa, host, port);
    defer list.deinit();

    const addr = for (list.addrs) |addr| {
        break addr;
    } else return error.AddressNotFound;

    return tcpConnectToAddr(rt, addr, userdata, msg, callback);
}

pub fn tcpConnectToAddr(
    rt: *io.Runtime,
    addr: std.net.Address,
    userdata: ?*anyopaque,
    msg: u16,
    callback: io.Callback,
) Allocator.Error!*ConnectTask {
    const conn = try rt.gpa.create(ConnectTask);
    errdefer rt.gpa.destroy(conn);

    conn.* = .{
        .userdata = userdata,
        .callback = callback,
        .msg = msg,

        .addr = addr,
        .fd = null,
        .task = undefined,
    };

    conn.task = try rt.socket(
        conn.addr.any.family,
        posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
        posix.IPPROTO.TCP,
        conn,
        @intFromEnum(ConnectTask.Msg.socket),
        ConnectTask.handleMsg,
    );

    return conn;
}

pub const ConnectTask = struct {
    userdata: ?*anyopaque,
    callback: io.Callback,
    msg: u16,

    addr: std.net.Address,
    fd: ?posix.fd_t,

    /// Task is the current task we are operating on. We store this to provide cancelation
    task: *io.Task,

    pub const Msg = enum {
        socket,
        connect,

        fn fromInt(v: u16) Msg {
            return @enumFromInt(v);
        }
    };

    /// Cancels the current task. Not guaranteed to actually cancel. User's callback will get an
    /// error.Canceled if cancelation was successful, otherwise the operation will complete as
    /// normal and this is essentially a no-op
    pub fn cancel(self: *ConnectTask, rt: *io.Runtime) void {
        _ = self.task.cancel(rt, null, 0, io.noopCallback) catch {};
    }

    pub fn handleMsg(ptr: ?*anyopaque, rt: *io.Runtime, msg: u16, result: io.Result) anyerror!void {
        const self = io.ptrCast(ConnectTask, ptr);
        switch (Msg.fromInt(msg)) {
            .socket => {
                assert(result == .socket);
                self.fd = result.socket catch |err| {
                    defer rt.gpa.destroy(self);
                    try self.callback(self.userdata, rt, self.msg, .{ .userfd = err });
                    return;
                };

                self.task = try rt.connect(
                    self.fd.?,
                    &self.addr.any,
                    self.addr.getOsSockLen(),
                    self,
                    @intFromEnum(Msg.connect),
                    ConnectTask.handleMsg,
                );
            },

            .connect => {
                assert(result == .connect);
                defer rt.gpa.destroy(self);

                _ = result.connect catch |err| {
                    try self.callback(self.userdata, rt, self.msg, .{ .userfd = err });
                    _ = try rt.close(self.fd.?, null, 0, io.noopCallback);
                    return;
                };

                try self.callback(self.userdata, rt, self.msg, .{ .userfd = self.fd.? });
            },
        }
    }
};

test "tcp connect" {
    var rt: io.Runtime = try .init(std.testing.allocator, 16);
    defer rt.deinit();

    const addr: std.net.Address = try .parseIp4("127.0.0.1", 80);

    {
        // Happy path
        const conn = try tcpConnectToAddr(&rt, addr, null, 0, io.noopCallback);
        errdefer std.testing.allocator.destroy(conn);

        const task1 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task1);
        try std.testing.expect(task1.req == .socket);
        try std.testing.expect(rt.submission_q.pop() == null);

        const fd: posix.fd_t = 7;
        try ConnectTask.handleMsg(conn, &rt, @intFromEnum(ConnectTask.Msg.socket), .{ .socket = fd });

        const task2 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task2);
        try std.testing.expect(task2.req == .connect);
        try std.testing.expect(rt.submission_q.pop() == null);

        try ConnectTask.handleMsg(conn, &rt, @intFromEnum(ConnectTask.Msg.connect), .{ .connect = {} });
        try std.testing.expect(rt.submission_q.pop() == null);
    }

    {
        // socket error
        const conn = try tcpConnectToAddr(&rt, addr, null, 0, io.noopCallback);
        errdefer std.testing.allocator.destroy(conn);

        const task1 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task1);

        try ConnectTask.handleMsg(conn, &rt, @intFromEnum(ConnectTask.Msg.socket), .{ .socket = error.Canceled });
        try std.testing.expect(rt.submission_q.pop() == null);
    }

    {
        // connect error
        const conn = try tcpConnectToAddr(&rt, addr, null, 0, io.noopCallback);
        errdefer std.testing.allocator.destroy(conn);

        const task1 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task1);
        try std.testing.expect(task1.req == .socket);
        try std.testing.expect(rt.submission_q.pop() == null);

        const fd: posix.fd_t = 7;
        try ConnectTask.handleMsg(conn, &rt, @intFromEnum(ConnectTask.Msg.socket), .{ .socket = fd });

        const task2 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task2);
        try std.testing.expect(task2.req == .connect);
        try std.testing.expect(rt.submission_q.pop() == null);

        try ConnectTask.handleMsg(conn, &rt, @intFromEnum(ConnectTask.Msg.connect), .{ .connect = error.Canceled });
        const task3 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task3);
        try std.testing.expect(task3.req == .close);
        try std.testing.expect(rt.submission_q.pop() == null);
    }
}
