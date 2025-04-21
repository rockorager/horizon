const std = @import("std");
const tls = @import("tls");
const io = @import("io.zig");

const Allocator = std.mem.Allocator;
const CertBundle = tls.config.cert.Bundle;
const assert = std.debug.assert;
const posix = std.posix;

pub const Client = struct {
    gpa: Allocator,
    fd: posix.fd_t,
    tls: tls.nonblock.Connection,

    read_buf: [tls.max_ciphertext_record_len]u8 = undefined,

    cleartext_buf: std.ArrayListUnmanaged(u8) = .empty,
    ciphertext_buf: std.ArrayListUnmanaged(u8) = .empty,

    pub const HandshakeTask = struct {
        userdata: ?*anyopaque,
        callback: io.Callback,
        msg: u16,

        fd: posix.fd_t,
        buffer: [tls.max_ciphertext_record_len]u8 = undefined,
        handshake: tls.nonblock.Client,
        task: *io.Task,

        pub fn handleMsg(
            ptr: ?*anyopaque,
            rt: *io.Runtime,
            _: u16,
            result: io.Result,
        ) anyerror!void {
            const self = io.ptrCast(HandshakeTask, ptr);

            switch (result) {
                .write => {
                    self.write_task = null;
                    _ = result.write catch |err| {
                        defer rt.gpa.destroy(self);
                        // send the error to the callback
                        try self.callback(self.userdata, rt, self.msg, .{ .userptr = err });
                        return;
                    };

                    if (self.handshake.done()) {
                        // Handshake is done. Create a client and deliver it to the callback
                        const client = try self.initClient(rt.gpa);
                        try self.callback(self.userdata, rt, self.msg, .{ .userptr = client });
                        rt.gpa.destroy(self);
                        return;
                    }

                    // Arm a recv task
                    self.task = try rt.recv(self.fd, &self.buffer, self, 0, handleMsg);
                },

                .recv => {
                    self.recv_task = null;
                    const n = result.recv catch |err| {
                        // send the error to the callback
                        try self.callback(self.userdata, rt, self.msg, .{ .userptr = err });
                        rt.gpa.destroy(self);
                        return;
                    };

                    const slice = self.buffer[0..n];
                    var scratch: [tls.max_ciphertext_record_len]u8 = undefined;
                    const r = try self.handshake.run(slice, &scratch);

                    if (r.send.len > 0) {
                        // Queue another send
                        @memcpy(self.buffer[0..r.send.len], r.send);
                        self.task = try rt.write(
                            self.fd,
                            self.buffer[0..r.send.len],
                            self,
                            0,
                            HandshakeTask.handleMsg,
                        );
                        return;
                    }

                    if (self.handshake.done()) {
                        // Handshake is done. Create a client and deliver it to the callback
                        const client = try self.initClient(rt.gpa);
                        try self.callback(self.userdata, rt, self.msg, .{ .userptr = client });
                        rt.gpa.destroy(self);
                        return;
                    }
                },

                else => unreachable,
            }
        }

        fn initClient(self: *HandshakeTask, gpa: Allocator) !*Client {
            const client = try gpa.create(Client);
            client.* = .{
                .gpa = gpa,
                .fd = self.fd,
                .tls = .{ .cipher = self.handshake.inner.cipher },
            };
            return client;
        }

        /// Tries to cancel the handshake. Callback will receive an error.Canceled if cancelation
        /// was successful, otherwise handhsake will proceed
        pub fn cancel(self: *HandshakeTask, rt: *io.Runtime) void {
            self.task.cancel(rt, null, 0, io.noopCallback) catch {};
        }
    };

    /// Initializes a handshake, which will ultimately deliver a Client to the callback
    pub fn init(
        rt: *io.Runtime,
        fd: posix.fd_t,
        opts: tls.config.Client,
        userdata: ?*anyopaque,
        msg: u16,
        callback: io.Callback,
    ) !*HandshakeTask {
        const hs = try rt.gpa.create(HandshakeTask);
        hs.* = .{
            .userdata = userdata,
            .callback = callback,
            .msg = msg,

            .fd = fd,
            .handshake = .init(opts),
            .task = undefined,
        };

        const result = try hs.handshake.run("", &hs.buffer);
        hs.task = try rt.write(hs.fd, result.send, hs, 0, HandshakeTask.handleMsg);
    }
};
