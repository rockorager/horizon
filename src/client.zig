const std = @import("std");
const horizon = @import("horizon.zig");
const io = @import("io");
const tls = @import("tls");

const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const CertBundle = tls.config.cert.Bundle;
const Event = horizon.Event;
const Worker = horizon.Worker;

const assert = std.debug.assert;
const log = std.log.scoped(.client);

pub const Client = struct {
    cert: CertBundle,

    pub const FetchOptions = struct {
        method: std.http.Method = .GET,
        url: []const u8,
        body: []const u8 = "",
        headers: std.StringHashMap([]const u8),
    };

    pub fn init(gpa: Allocator) !Client {
        return .{
            .cert = try tls.config.cert.fromSystem(gpa),
        };
    }
};

pub const Request = struct {
    arena: std.heap.ArenaAllocator,

    addr: std.net.Address,
    method: std.http.Method = .GET,

    headers: std.StringHashMapUnmanaged([]const u8) = .empty,
    body: std.ArrayListUnmanaged(u8) = .empty,

    // pub fn init(gpa: Allocator, method: std.http.Method, url: []const u8) !Request {
    //     const addrs = try std.net.getAddressList(gpa, url, 443);
    //     defer addrs.deinit();
    // }
};

pub const Connection = struct {
    arena: std.mem.Allocator,

    recv_buf: [1024]u8 = undefined,

    resp_buf: std.ArrayListUnmanaged(u8) = .empty,
    send_buf: std.ArrayListUnmanaged(u8) = .empty,
    written: usize = 0,

    op_c: horizon.Event = .{ .parent = .client_connection, .op = .socket },
    send_c: horizon.Event = .{ .parent = .client_connection, .op = .send },
    state: State = .init,
    addr: net.Address,

    tls_client: union(enum) {
        none,
        handshake: tls.nonblock.Client,
        conn: tls.nonblock.Connection,
    },

    fd: posix.fd_t = undefined,

    const State = enum {
        init,
        socket_response,
        connect_response,
        handshake,
        send_request,
    };

    pub fn init(
        self: *Connection,
        arena: Allocator,
        bundle: CertBundle,
        worker: *Worker,
        buf: []const u8,
        port: u16,
    ) !void {
        const list = try std.net.getAddressList(arena, buf, port);
        if (list.addrs.len == 0) @panic("here");
        const addr = list.addrs[0];

        const opts: tls.config.Client = .{
            .host = buf,
            .root_ca = bundle,
        };

        self.* = .{
            .arena = arena,
            .addr = addr,
            .tls_client = .{ .handshake = .init(opts) },
        };

        try worker.ring.socket(
            self.addr.any.family,
            posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
            posix.IPPROTO.TCP,
            @intFromPtr(&self.op_c),
        );
        self.state = .socket_response;
    }

    pub fn onEvent(
        self: *Connection,
        worker: *Worker,
        cqe: io.Completion,
        event: *Event,
    ) !void {
        state: switch (self.state) {
            .init => unreachable,
            .socket_response => {
                assert(event.op == .socket);

                self.fd = cqe.unwrap() catch |err| {
                    log.err("socket error: {}", .{err});
                    return err;
                };

                self.op_c.op = .connect;

                try worker.ring.connect(
                    self.fd,
                    &self.addr.any,
                    self.addr.getOsSockLen(),
                    @intFromPtr(&self.op_c),
                );

                self.state = .connect_response;
                return;
            },

            .connect_response => {
                assert(event.op == .connect);

                _ = cqe.unwrap() catch |err| {
                    log.err("connect error: {}", .{err});
                    return err;
                };

                // Once we are connected, we send the client hello and start receiving
                const hs = &self.tls_client.handshake;
                var buf: [tls.max_ciphertext_record_len]u8 = undefined;
                const result = try hs.run("", &buf);
                try self.send_buf.appendSlice(self.arena, result.send);
                try worker.ring.write(self.fd, self.send_buf.items, &self.send_c);

                self.op_c.op = .recv;
                try worker.ring.recv(self.fd, &self.recv_buf, &self.op_c);

                self.state = .handshake;
                return;
            },

            .handshake => {
                assert(self.tls_client == .handshake);
                const hs = &self.tls_client.handshake;
                log.debug("cqe {}", .{event.op});
                switch (event.op) {
                    .send => {
                        const result = cqe.unwrap() catch |err| {
                            log.err("send during handshake error: {}", .{err});
                            posix.close(self.fd);
                            return;
                        };
                        self.written += result;
                        if (self.written < self.send_buf.items.len) {
                            try worker.ring.write(
                                self.fd,
                                self.send_buf.items[self.written..],
                                &self.send_c,
                            );
                        } else {
                            self.written = 0;
                            self.send_buf.clearRetainingCapacity();
                        }
                    },
                    .recv => {
                        const result = cqe.unwrap() catch |err| {
                            log.err("recv during handshake error: {}", .{err});
                            posix.close(self.fd);
                            return;
                        };

                        try self.resp_buf.appendSlice(self.arena, self.recv_buf[0..result]);
                        var send_buf: [tls.max_ciphertext_record_len]u8 = undefined;
                        const r = try hs.run(self.resp_buf.items, &send_buf);
                        self.resp_buf.replaceRangeAssumeCapacity(0, r.recv_pos, "");

                        try worker.ring.recv(self.fd, &self.recv_buf, &self.op_c);

                        if (self.tls_client.handshake.done() and r.send.len == 0) {
                            assert(self.written == 0);
                            assert(self.send_buf.items.len == 0);
                            log.debug("handshake done, to write={d}", .{r.send.len});
                        }

                        if (r.send.len > 0) {
                            try self.send_buf.appendSlice(self.arena, r.send);
                            try worker.ring.write(
                                self.fd,
                                self.send_buf.items[self.written..],
                                &self.send_c,
                            );
                        }

                        if (!hs.done()) return;

                        // Now send the request body
                        self.tls_client = .{ .conn = .init(hs.inner.cipher) };
                        self.state = .send_request;

                        continue :state .send_request;
                    },

                    else => unreachable,
                }
            },

            .send_request => {},
        }
    }
};
