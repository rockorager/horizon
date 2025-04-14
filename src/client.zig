const std = @import("std");
const io = @import("io");
const tls = @import("tls");

const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const CertBundle = tls.config.cert.Bundle;

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
    arena: std.mem.Allocator,

    url: []const u8,
    method: std.http.Method = .GET,

    headers: std.StringHashMapUnmanaged([]const u8) = .empty,
    body: std.ArrayListUnmanaged(u8) = .empty,

    pub fn init(arena: Allocator, method: std.http.Method, url: []const u8) Request {
        return .{
            .arena = arena,
            .url = url,
            .method = method,
        };
    }
};

pub const Connection = struct {
    arena: std.mem.Allocator,

    recv_buf: [1024]u8 = undefined,

    resp_buf: std.ArrayListUnmanaged(u8) = .empty,
    send_buf: std.ArrayListUnmanaged(u8) = .empty,
    written: usize = 0,

    state: State = .init,
    addr: net.Address,
    uri: std.Uri,

    tls_client: union(enum) {
        none,
        handshake: tls.nonblock.Client,
        conn: tls.nonblock.Connection,
    },

    fd: posix.fd_t = undefined,

    request: Request,

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
        ioring: *io.Ring,
        request: Request,
    ) !void {
        const uri = try std.Uri.parse(request.url);

        const port: u16 = uri.port orelse
            if (std.mem.eql(u8, uri.scheme, "https") or std.mem.eql(u8, uri.scheme, "wss"))
                443
            else if (std.mem.eql(u8, uri.scheme, "http") or std.mem.eql(u8, uri.scheme, "ws"))
                80
            else
                return error.UnsupportedProtocol;

        const host = try uri.host.?.toRawMaybeAlloc(arena);
        const list = try std.net.getAddressList(arena, host, port);
        if (list.addrs.len == 0) return error.AddressNotFound;
        const addr = list.addrs[0];

        const opts: tls.config.Client = .{
            .host = host,
            .root_ca = bundle,
        };

        self.* = .{
            .arena = arena,
            .uri = uri,
            .addr = addr,
            .tls_client = .{ .handshake = .init(opts) },
            .request = request,
        };

        _ = try ioring.socket(
            self.addr.any.family,
            posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
            posix.IPPROTO.TCP,
            self,
            Connection.onTaskCompletion,
        );
        self.state = .socket_response;
    }

    fn onTaskCompletion(
        ring: *io.Ring,
        task: *io.Task,
        result: io.Result,
    ) anyerror!void {
        const self = task.ptrCast(Connection);
        state: switch (self.state) {
            .init => unreachable,

            .socket_response => {
                assert(result == .socket);

                self.fd = result.socket catch |err| {
                    log.err("socket error: {}", .{err});
                    return err;
                };

                _ = try ring.connect(
                    self.fd,
                    &self.addr.any,
                    self.addr.getOsSockLen(),
                    self,
                    Connection.onTaskCompletion,
                );

                self.state = .connect_response;
            },

            .connect_response => {
                assert(result == .connect);

                _ = result.connect catch |err| {
                    log.err("socket error: {}", .{err});
                    _ = try ring.close(self.fd, null, io.noopCallback);
                    return err;
                };

                // Kick off the handshake
                const hs = &self.tls_client.handshake;
                var buf: [tls.max_ciphertext_record_len]u8 = undefined;

                const hs_result = try hs.run("", &buf);
                try self.send_buf.appendSlice(self.arena, hs_result.send);

                // Issue the write
                _ = try ring.write(self.fd, self.send_buf.items, self, Connection.onTaskCompletion);

                // Start receiving
                _ = try ring.recv(self.fd, &self.recv_buf, self, Connection.onTaskCompletion);

                self.state = .handshake;
            },

            .handshake => {
                assert(self.tls_client == .handshake);
                const hs = &self.tls_client.handshake;
                switch (result) {
                    .write => {
                        const n = result.write catch |err| {
                            log.err("send during handshake error: {}", .{err});
                            _ = try ring.close(self.fd, null, io.noopCallback);
                            return;
                        };

                        self.written += n;
                        if (self.written < self.send_buf.items.len) {
                            log.err("handshake short write", .{});
                            _ = try ring.write(
                                self.fd,
                                self.send_buf.items[self.written..],
                                self,
                                Connection.onTaskCompletion,
                            );
                        } else {
                            log.err("handshake full write", .{});
                            self.written = 0;
                            self.send_buf.clearRetainingCapacity();
                        }
                    },

                    .recv => {
                        const n = result.recv catch |err| {
                            log.err("recv during handshake error: {}", .{err});
                            _ = try ring.close(self.fd, null, io.noopCallback);
                            return;
                        };

                        // Buffer the response
                        try self.resp_buf.appendSlice(self.arena, self.recv_buf[0..n]);

                        // Run the handshake
                        var send_buf: [tls.max_ciphertext_record_len]u8 = undefined;
                        const r = try hs.run(self.resp_buf.items, &send_buf);

                        // Swallow the bytes we used for the handshake, if any
                        self.resp_buf.replaceRangeAssumeCapacity(0, r.recv_pos, "");

                        // Any send we add directly to our buffer
                        try self.send_buf.appendSlice(self.arena, r.send);

                        // Rearm our recv task
                        _ = try ring.recv(
                            self.fd,
                            &self.recv_buf,
                            self,
                            Connection.onTaskCompletion,
                        );

                        // If the handshake is done, we can go straight to the send request state.
                        // It's possible we have some handshake bytes left to send, but those are
                        // already buffered and can be sent with the request
                        if (hs.done()) {
                            self.tls_client = .{ .conn = .init(hs.inner.cipher) };
                            try self.prepRequest();
                            self.state = .send_request;
                            continue :state .send_request;
                        }

                        // if we aren't done, we need to make sure we arm a write if needed
                        if (r.send.len > 0) {
                            _ = try ring.write(
                                self.fd,
                                self.send_buf.items[self.written..],
                                self,
                                Connection.onTaskCompletion,
                            );
                        }
                    },

                    else => unreachable,
                }
            },

            .send_request => @panic("here"),
        }
    }

    fn prepRequest(self: *Connection) !void {
        self.send_buf.clearRetainingCapacity();
        var writer = self.send_buf.writer(self.arena);
        self.request.method = .GET;
        try self.request.method.write(writer);
        const path = self.uri.path.percent_encoded;
        try writer.print(" {s} HTTP/1.1\r\n", .{if (path.len == 0) "/" else path});
        log.err("{s}", .{self.send_buf.items});
    }
};

test "client: handshake" {
    var ring = try io.Ring.init(std.testing.allocator, 8);
    var client: Client = try .init(std.testing.allocator);
    defer client.cert.deinit(std.testing.allocator);

    var conn: Connection = undefined;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const req = Request.init(arena.allocator(), .GET, "https://timculverhouse.com/");
    try conn.init(arena.allocator(), client.cert, &ring, req);

    try ring.run(.until_done);
}
