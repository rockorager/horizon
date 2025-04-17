const std = @import("std");
const io = @import("io");
const tls = @import("tls");

const http = std.http;
const net = std.net;
const posix = std.posix;
const proto = std.http.protocol;
const Allocator = std.mem.Allocator;
const CertBundle = tls.config.cert.Bundle;
const Uri = std.Uri;

const assert = std.debug.assert;
const log = std.log.scoped(.client);

pub const Client = struct {
    gpa: Allocator,
    cert: CertBundle,
    connection_pool: ConnectionPool = .{},

    pub const FetchOptions = struct {
        server_header_buffer: ?[]u8 = null,
        redirect_behavior: ?Request.RedirectBehavior = null,

        /// If the server sends a body, it will be appended to this ArrayList.
        /// `max_append_size` provides an upper limit for how much they can grow.
        response_storage: ResponseStorage = .ignore,
        max_append_size: ?usize = null,

        location: Location,
        method: ?http.Method = null,
        payload: ?[]const u8 = null,
        raw_uri: bool = false,
        keep_alive: bool = true,

        /// Standard headers that have default, but overridable, behavior.
        headers: Request.Headers = .{},
        /// These headers are kept including when following a redirect to a
        /// different domain.
        /// Externally-owned; must outlive the Request.
        extra_headers: []const http.Header = &.{},
        /// These headers are stripped when following a redirect to a different
        /// domain.
        /// Externally-owned; must outlive the Request.
        privileged_headers: []const http.Header = &.{},

        pub const Location = union(enum) {
            url: []const u8,
            uri: Uri,
        };

        pub const ResponseStorage = union(enum) {
            ignore,
            /// Only the existing capacity will be used.
            static: *std.ArrayListUnmanaged(u8),
            dynamic: *std.ArrayList(u8),
        };
    };

    pub fn init(gpa: Allocator) !Client {
        return .{
            .gpa = gpa,
            .cert = try tls.config.cert.fromSystem(gpa),
            .connection_pool = .{},
        };
    }

    pub fn deinit(self: *Client) void {
        self.cert.deinit(self.gpa);
        self.connection_pool.deinit(self.gpa);
    }

    pub fn fetch(
        self: *Client,
        arena: Allocator,
        ring: *io.Runtime,
        options: FetchOptions,
        userdata: ?*anyopaque,
        callback: *const fn (*Request) anyerror!void,
    ) !*Request {
        const uri = switch (options.location) {
            .url => |u| try Uri.parse(u),
            .uri => |u| u,
        };
        var server_header_buffer: [16 * 1024]u8 = undefined;

        const method: std.http.Method = options.method orelse
            if (options.payload != null) .POST else .GET;

        const req = try open(self, arena, method, uri, .{
            .server_header_buffer = options.server_header_buffer orelse &server_header_buffer,
            .redirect_behavior = options.redirect_behavior orelse
                if (options.payload == null) @enumFromInt(3) else .unhandled,
            .headers = options.headers,
            .extra_headers = options.extra_headers,
            .privileged_headers = options.privileged_headers,
            .keep_alive = options.keep_alive,
        }, userdata, callback, ring);

        if (options.payload) |payload| req.transfer_encoding = .{ .content_length = payload.len };

        req.payload = options.payload;

        return req;
    }

    pub fn connect(
        self: *Client,
        host: []const u8,
        port: u16,
        protocol: Connection.Protocol,
        ring: *io.Runtime,
    ) !*Connection {
        if (self.connection_pool.findConnection(.{
            .host = host,
            .port = port,
            .protocol = protocol,
        })) |node| return node;

        const conn = try self.gpa.create(ConnectionPool.Node);
        errdefer self.gpa.destroy(conn);
        conn.* = .{ .data = undefined };
        try conn.data.init(
            self.gpa,
            try self.gpa.dupe(u8, host),
            port,
            protocol,
            self.cert,
            ring,
        );

        self.connection_pool.addUsed(conn);

        return &conn.data;
    }
};

pub const Connection = struct {
    gpa: std.mem.Allocator,

    recv_buf: [4096 * 4]u8 = undefined,

    resp_buf: std.ArrayListUnmanaged(u8) = .empty,
    send_buf: std.ArrayListUnmanaged(u8) = .empty,

    state: State = .init,
    addr: net.Address,

    tls_client: union(enum) {
        none,
        handshake: tls.nonblock.Client,
        conn: tls.nonblock.Connection,
    },

    fd: posix.fd_t = undefined,

    written: usize = 0,

    host: []const u8,
    port: u16,
    protocol: Protocol,

    request: ?*Request,

    proxied: bool = false,
    closing: bool = false,

    pub const Protocol = enum { plain, tls };

    const State = enum {
        init,
        socket_response,
        connect_response,
        handshake,
        send_request,
        waiting_response,
        idle,
    };

    pub fn init(
        self: *Connection,
        gpa: Allocator,
        host: []const u8,
        port: u16,
        protocol: Connection.Protocol,
        bundle: CertBundle,
        ioring: *io.Runtime,
    ) !void {
        const list = try std.net.getAddressList(gpa, host, port);
        defer list.deinit();
        if (list.addrs.len == 0) return error.AddressNotFound;
        const addr = list.addrs[0];

        const opts: tls.config.Client = .{
            .host = host,
            .root_ca = bundle,
        };

        self.* = .{
            .gpa = gpa,
            .addr = addr,
            .tls_client = .{ .handshake = .init(opts) },
            .host = host,
            .port = port,
            .protocol = protocol,
            .request = null,
        };

        _ = try ioring.socket(
            self.addr.any.family,
            posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
            posix.IPPROTO.TCP,
            self,
            0,
            Connection.onTaskCompletion,
        );
        self.state = .socket_response;
    }

    fn writer(self: *Connection) std.ArrayListUnmanaged(u8).Writer {
        return self.send_buf.writer(self.gpa);
    }

    fn onTaskCompletion(
        ptr: ?*anyopaque,
        ring: *io.Runtime,
        _: u16,
        result: io.Result,
    ) anyerror!void {
        const self = io.ptrCast(Connection, ptr);
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
                    0,
                    Connection.onTaskCompletion,
                );

                self.state = .connect_response;
            },

            .connect_response => {
                assert(result == .connect);

                _ = result.connect catch |err| {
                    log.err("socket error: {}", .{err});
                    _ = try ring.close(self.fd, null, 0, io.noopCallback);
                    return err;
                };

                // Kick off the handshake
                const hs = &self.tls_client.handshake;
                var buf: [tls.max_ciphertext_record_len]u8 = undefined;

                const hs_result = try hs.run("", &buf);
                try self.send_buf.appendSlice(self.gpa, hs_result.send);

                // Issue the write
                _ = try ring.write(self.fd, self.send_buf.items, self, 0, Connection.onTaskCompletion);

                // Start receiving
                _ = try ring.recv(self.fd, &self.recv_buf, self, 0, Connection.onTaskCompletion);

                self.state = .handshake;
            },

            .handshake => {
                assert(self.tls_client == .handshake);
                const hs = &self.tls_client.handshake;
                switch (result) {
                    .write => {
                        const n = result.write catch |err| {
                            log.err("send during handshake error: {}", .{err});
                            _ = try ring.close(self.fd, null, 0, io.noopCallback);
                            return;
                        };

                        self.written += n;
                        if (self.written < self.send_buf.items.len) {
                            _ = try ring.write(
                                self.fd,
                                self.send_buf.items[self.written..],
                                self,
                                0,
                                Connection.onTaskCompletion,
                            );
                        } else {
                            self.written = 0;
                            self.send_buf.clearRetainingCapacity();
                        }

                        if (hs.done()) {
                            self.tls_client = .{ .conn = .init(hs.inner.cipher) };
                            self.state = .send_request;
                            continue :state .send_request;
                        }
                    },

                    .recv => {
                        const n = result.recv catch |err| {
                            log.err("recv during handshake error: {}", .{err});
                            _ = try ring.close(self.fd, null, 0, io.noopCallback);
                            return;
                        };

                        // Buffer the response
                        try self.resp_buf.appendSlice(self.gpa, self.recv_buf[0..n]);

                        // Run the handshake
                        var send_buf: [tls.max_ciphertext_record_len]u8 = undefined;
                        const r = try hs.run(self.resp_buf.items, &send_buf);

                        // Swallow the bytes we used for the handshake, if any
                        self.resp_buf.replaceRangeAssumeCapacity(0, r.recv_pos, "");

                        // Any send we add directly to our buffer
                        try self.send_buf.appendSlice(self.gpa, r.send);

                        // Rearm our recv task
                        _ = try ring.recv(
                            self.fd,
                            &self.recv_buf,
                            self,
                            0,
                            Connection.onTaskCompletion,
                        );

                        // If the handshake is done, we can go straight to the send request state.
                        // It's possible we have some handshake bytes left to send, but those are
                        // already buffered and can be sent with the request
                        if (hs.done() and self.send_buf.items.len == 0) {
                            self.tls_client = .{ .conn = .init(hs.inner.cipher) };
                            self.state = .send_request;
                            continue :state .send_request;
                        }

                        // if we aren't done, we need to make sure we arm a write if needed
                        if (r.send.len > 0) {
                            _ = try ring.write(
                                self.fd,
                                self.send_buf.items[self.written..],
                                self,
                                0,
                                Connection.onTaskCompletion,
                            );
                        }
                    },

                    else => unreachable,
                }
            },

            .send_request => {
                if (self.request) |req| {
                    try req.send();
                    const n = self.tls_client.conn.encryptedLength(self.send_buf.items.len);
                    const buf = try req.arena.alloc(u8, n);
                    const r = try self.tls_client.conn.encrypt(self.send_buf.items, buf);
                    if (r.unused_cleartext.len > 0) @panic("TODO");
                    _ = try ring.write(
                        self.fd,
                        r.ciphertext,
                        self,
                        0,
                        Connection.onTaskCompletion,
                    );
                    self.state = .waiting_response;
                }
            },

            .waiting_response => {
                switch (result) {
                    .write => {
                        const n = result.write catch |err| {
                            log.err("send error: {}", .{err});
                            _ = try ring.close(self.fd, null, 0, io.noopCallback);
                            return;
                        };

                        self.written += n;
                        if (self.written < self.send_buf.items.len) {
                            _ = try ring.write(
                                self.fd,
                                self.send_buf.items[self.written..],
                                self,
                                0,
                                Connection.onTaskCompletion,
                            );
                        } else {
                            self.written = 0;
                            self.send_buf.clearRetainingCapacity();
                        }
                    },

                    .recv => {
                        const n = result.recv catch |err| {
                            log.err("recv during handshake error: {}", .{err});
                            _ = try ring.close(self.fd, null, 0, io.noopCallback);
                            return;
                        };

                        const slice = switch (self.tls_client) {
                            .none => self.recv_buf[0..n],
                            .handshake => unreachable,
                            .conn => |*conn| blk: {
                                const r = try conn.decrypt(self.recv_buf[0..n], self.recv_buf[0..n]);
                                break :blk r.cleartext;
                            },
                        };

                        const req = self.request.?;
                        try req.response.appendSlice(req.arena, slice);

                        if (req.response.headLen() == null) {
                            // Rearm our recv task
                            _ = try ring.recv(
                                self.fd,
                                &self.recv_buf,
                                self,
                                0,
                                Connection.onTaskCompletion,
                            );
                            return;
                        }

                        // TODO:check if we have the full request, then keep reading
                        try req.callback(req);

                        self.state = .idle;
                        req.client.connection_pool.release(req.client.gpa, self);
                    },

                    else => unreachable,
                }
            },

            .idle => {},
        }
    }

    pub fn close(self: *Connection) void {
        switch (self.tls_client) {
            .conn => |*conn| blk: {
                var buf: [8]u8 = undefined;
                const msg = conn.close(&buf) catch break :blk;
                _ = posix.write(self.fd, msg) catch break :blk;
            },
            else => {},
        }
        posix.close(self.fd);
        self.send_buf.deinit(self.gpa);
        self.resp_buf.deinit(self.gpa);
    }
};

pub const Request = struct {
    arena: Allocator,

    ptr: ?*anyopaque,
    callback: *const fn (*Request) anyerror!void,

    payload: ?[]const u8 = null,

    uri: std.Uri,
    client: *Client,
    /// This is null when the connection is released.
    connection: ?*Connection,
    keep_alive: bool,

    method: std.http.Method,
    version: std.http.Version = .@"HTTP/1.1",
    transfer_encoding: std.http.Client.RequestTransfer,
    redirect_behavior: RedirectBehavior,

    /// Whether the request should handle a 100-continue response before sending the request body.
    handle_continue: bool,

    /// The response associated with this request.
    response: Response,

    /// Standard headers that have default, but overridable, behavior.
    headers: Headers,

    /// These headers are kept including when following a redirect to a
    /// different domain.
    /// Externally-owned; must outlive the Request.
    extra_headers: []const std.http.Header,

    /// These headers are stripped when following a redirect to a different
    /// domain.
    /// Externally-owned; must outlive the Request.
    privileged_headers: []const std.http.Header,

    pub const Headers = struct {
        host: Value = .default,
        authorization: Value = .default,
        user_agent: Value = .default,
        connection: Value = .default,
        accept_encoding: Value = .default,
        content_type: Value = .default,

        pub const Value = union(enum) {
            default,
            omit,
            override: []const u8,
        };
    };

    /// Any value other than `not_allowed` or `unhandled` means that integer represents
    /// how many remaining redirects are allowed.
    pub const RedirectBehavior = enum(u16) {
        /// The next redirect will cause an error.
        not_allowed = 0,
        /// Redirects are passed to the client to analyze the redirect response
        /// directly.
        unhandled = std.math.maxInt(u16),
        _,

        pub fn subtractOne(rb: *RedirectBehavior) void {
            switch (rb.*) {
                .not_allowed => unreachable,
                .unhandled => unreachable,
                _ => rb.* = @enumFromInt(@intFromEnum(rb.*) - 1),
            }
        }

        pub fn remaining(rb: RedirectBehavior) u16 {
            assert(rb != .unhandled);
            return @intFromEnum(rb);
        }
    };

    /// Frees all resources associated with the request.
    pub fn deinit(req: *Request) void {
        if (req.connection) |connection| {
            req.client.connection_pool.release(req.client.gpa, connection);
        }
        req.* = undefined;
    }

    // This function must deallocate all resources associated with the request,
    // or keep those which will be used.
    // This needs to be kept in sync with deinit and request.
    fn redirect(req: *Request, uri: std.Uri) !void {
        assert(req.response.parser.done);

        req.client.connection_pool.release(req.client.allocator, req.connection.?);
        req.connection = null;

        var server_header: std.heap.FixedBufferAllocator = .init(req.response.parser.header_bytes_buffer);
        defer req.response.parser.header_bytes_buffer = server_header.buffer[server_header.end_index..];
        const protocol, const valid_uri = try validateUri(uri, server_header.allocator());

        const new_host = valid_uri.host.?.raw;
        const prev_host = req.uri.host.?.raw;
        const keep_privileged_headers =
            std.ascii.eqlIgnoreCase(valid_uri.scheme, req.uri.scheme) and
            std.ascii.endsWithIgnoreCase(new_host, prev_host) and
            (new_host.len == prev_host.len or new_host[new_host.len - prev_host.len - 1] == '.');
        if (!keep_privileged_headers) {
            // When redirecting to a different domain, strip privileged headers.
            req.privileged_headers = &.{};
        }

        if (switch (req.response.status) {
            .see_other => true,
            .moved_permanently, .found => req.method == .POST,
            else => false,
        }) {
            // A redirect to a GET must change the method and remove the body.
            req.method = .GET;
            req.transfer_encoding = .none;
            req.headers.content_type = .omit;
        }

        if (req.transfer_encoding != .none) {
            // The request body has already been sent. The request is
            // still in a valid state, but the redirect must be handled
            // manually.
            return error.RedirectRequiresResend;
        }

        req.uri = valid_uri;
        req.connection = try req.client.connect(new_host, uriPort(valid_uri, protocol), protocol);
        req.redirect_behavior.subtractOne();
        req.response.parser.reset();

        req.response = .{
            .version = undefined,
            .status = undefined,
            .reason = undefined,
            .keep_alive = undefined,
            .parser = req.response.parser,
        };
    }

    /// Send the HTTP request headers to the server.
    pub fn send(req: *Request) !void {
        if (!req.method.requestHasBody() and req.transfer_encoding != .none)
            return error.UnsupportedTransferEncoding;

        const connection = req.connection.?;
        const w = connection.writer();

        try req.method.write(w);
        try w.writeByte(' ');

        if (req.method == .CONNECT) {
            try req.uri.writeToStream(.{ .authority = true }, w);
        } else {
            try req.uri.writeToStream(.{
                .scheme = connection.proxied,
                .authentication = connection.proxied,
                .authority = connection.proxied,
                .path = true,
                .query = true,
            }, w);
        }
        try w.writeByte(' ');
        try w.writeAll(@tagName(req.version));
        try w.writeAll("\r\n");

        if (try emitOverridableHeader("host: ", req.headers.host, w)) {
            try w.writeAll("host: ");
            try req.uri.writeToStream(.{ .authority = true }, w);
            try w.writeAll("\r\n");
        }

        if (try emitOverridableHeader("authorization: ", req.headers.authorization, w)) {
            if (req.uri.user != null or req.uri.password != null) {
                try w.writeAll("authorization: ");
                try basic_authorization.writeTo(req.uri, w);
                try w.writeAll("\r\n");
            }
        }

        if (try emitOverridableHeader("user-agent: ", req.headers.user_agent, w)) {
            try w.writeAll("User-Agent: horizon/0.0.0-dev\r\n");
        }

        if (try emitOverridableHeader("connection: ", req.headers.connection, w)) {
            if (req.keep_alive) {
                try w.writeAll("connection: keep-alive\r\n");
            } else {
                try w.writeAll("connection: close\r\n");
            }
        }

        if (try emitOverridableHeader("accept-encoding: ", req.headers.accept_encoding, w)) {
            // https://github.com/ziglang/zig/issues/18937
            //try w.writeAll("accept-encoding: gzip, deflate, zstd\r\n");
            try w.writeAll("accept-encoding: gzip\r\n");
        }

        switch (req.transfer_encoding) {
            .chunked => try w.writeAll("transfer-encoding: chunked\r\n"),
            .content_length => |len| try w.print("content-length: {d}\r\n", .{len}),
            .none => {},
        }

        if (try emitOverridableHeader("content-type: ", req.headers.content_type, w)) {
            // The default is to omit content-type if not provided because
            // "application/octet-stream" is redundant.
        }

        for (req.extra_headers) |header| {
            assert(header.name.len != 0);

            try w.writeAll(header.name);
            try w.writeAll(": ");
            try w.writeAll(header.value);
            try w.writeAll("\r\n");
        }

        // if (connection.proxied) proxy: {
        //     const proxy = switch (connection.protocol) {
        //         .plain => req.client.http_proxy,
        //         .tls => req.client.https_proxy,
        //     } orelse break :proxy;
        //
        //     const authorization = proxy.authorization orelse break :proxy;
        //     try w.writeAll("proxy-authorization: ");
        //     try w.writeAll(authorization);
        //     try w.writeAll("\r\n");
        // }

        try w.writeAll("\r\n");

        if (req.payload) |payload| try w.writeAll(payload);
    }

    /// Returns true if the default behavior is required, otherwise handles
    /// writing (or not writing) the header.
    fn emitOverridableHeader(prefix: []const u8, v: Headers.Value, w: anytype) !bool {
        switch (v) {
            .default => return true,
            .omit => return false,
            .override => |x| {
                try w.writeAll(prefix);
                try w.writeAll(x);
                try w.writeAll("\r\n");
                return false;
            },
        }
    }

    const TransferReadError = Connection.ReadError || proto.HeadersParser.ReadError;

    const TransferReader = std.io.Reader(*Request, TransferReadError, transferRead);

    fn transferReader(req: *Request) TransferReader {
        return .{ .context = req };
    }

    fn transferRead(req: *Request, buf: []u8) TransferReadError!usize {
        if (req.response.parser.done) return 0;

        var index: usize = 0;
        while (index == 0) {
            const amt = try req.response.parser.read(req.connection.?, buf[index..], req.response.skip);
            if (amt == 0 and req.response.parser.done) break;
            index += amt;
        }

        return index;
    }

    /// Waits for a response from the server and parses any headers that are sent.
    /// This function will block until the final response is received.
    ///
    /// If handling redirects and the request has no payload, then this
    /// function will automatically follow redirects. If a request payload is
    /// present, then this function will error with
    /// error.RedirectRequiresResend.
    ///
    /// Must be called after `send` and, if any data was written to the request
    /// body, then also after `finish`.
    pub fn wait(req: *Request) !void {
        while (true) {
            // This while loop is for handling redirects, which means the request's
            // connection may be different than the previous iteration. However, it
            // is still guaranteed to be non-null with each iteration of this loop.
            const connection = req.connection.?;

            while (true) { // read headers
                try connection.fill();

                const nchecked = try req.response.parser.checkCompleteHead(connection.peek());
                connection.drop(@intCast(nchecked));

                if (req.response.parser.state.isContent()) break;
            }

            try req.response.parse(req.response.parser.get());

            if (req.response.status == .@"continue") {
                // We're done parsing the continue response; reset to prepare
                // for the real response.
                req.response.parser.done = true;
                req.response.parser.reset();

                if (req.handle_continue)
                    continue;

                return; // we're not handling the 100-continue
            }

            // we're switching protocols, so this connection is no longer doing http
            if (req.method == .CONNECT and req.response.status.class() == .success) {
                connection.closing = false;
                req.response.parser.done = true;
                return; // the connection is not HTTP past this point
            }

            connection.closing = !req.response.keep_alive or !req.keep_alive;

            // Any response to a HEAD request and any response with a 1xx
            // (Informational), 204 (No Content), or 304 (Not Modified) status
            // code is always terminated by the first empty line after the
            // header fields, regardless of the header fields present in the
            // message.
            if (req.method == .HEAD or req.response.status.class() == .informational or
                req.response.status == .no_content or req.response.status == .not_modified)
            {
                req.response.parser.done = true;
                return; // The response is empty; no further setup or redirection is necessary.
            }

            switch (req.response.transfer_encoding) {
                .none => {
                    if (req.response.content_length) |cl| {
                        req.response.parser.next_chunk_length = cl;

                        if (cl == 0) req.response.parser.done = true;
                    } else {
                        // read until the connection is closed
                        req.response.parser.next_chunk_length = std.math.maxInt(u64);
                    }
                },
                .chunked => {
                    req.response.parser.next_chunk_length = 0;
                    req.response.parser.state = .chunk_head_size;
                },
            }

            if (req.response.status.class() == .redirect and req.redirect_behavior != .unhandled) {
                // skip the body of the redirect response, this will at least
                // leave the connection in a known good state.
                req.response.skip = true;
                assert(try req.transferRead(&.{}) == 0); // we're skipping, no buffer is necessary

                if (req.redirect_behavior == .not_allowed) return error.TooManyHttpRedirects;

                const location = req.response.location orelse
                    return error.HttpRedirectLocationMissing;

                // This mutates the beginning of header_bytes_buffer and uses that
                // for the backing memory of the returned Uri.
                try req.redirect(req.uri.resolve_inplace(
                    location,
                    &req.response.parser.header_bytes_buffer,
                ) catch |err| switch (err) {
                    error.UnexpectedCharacter,
                    error.InvalidFormat,
                    error.InvalidPort,
                    => return error.HttpRedirectLocationInvalid,
                    error.NoSpaceLeft => return error.HttpHeadersOversize,
                });
                try req.send();
            } else {
                req.response.skip = false;
                if (!req.response.parser.done) {
                    switch (req.response.transfer_compression) {
                        .identity => req.response.compression = .none,
                        .compress, .@"x-compress" => return error.CompressionUnsupported,
                        .deflate => req.response.compression = .{
                            .deflate = std.compress.zlib.decompressor(req.transferReader()),
                        },
                        .gzip, .@"x-gzip" => req.response.compression = .{
                            .gzip = std.compress.gzip.decompressor(req.transferReader()),
                        },
                        // https://github.com/ziglang/zig/issues/18937
                        //.zstd => req.response.compression = .{
                        //    .zstd = std.compress.zstd.decompressStream(req.client.allocator, req.transferReader()),
                        //},
                        .zstd => return error.CompressionUnsupported,
                    }
                }

                break;
            }
        }
    }

    pub const ReadError = TransferReadError || proto.HeadersParser.CheckCompleteHeadError ||
        error{ DecompressionFailure, InvalidTrailers };

    pub const Reader = std.io.Reader(*Request, ReadError, read);

    pub fn reader(req: *Request) Reader {
        return .{ .context = req };
    }

    /// Reads data from the response body. Must be called after `wait`.
    pub fn read(req: *Request, buffer: []u8) ReadError!usize {
        const out_index = switch (req.response.compression) {
            .deflate => |*deflate| deflate.read(buffer) catch return error.DecompressionFailure,
            .gzip => |*gzip| gzip.read(buffer) catch return error.DecompressionFailure,
            // https://github.com/ziglang/zig/issues/18937
            //.zstd => |*zstd| zstd.read(buffer) catch return error.DecompressionFailure,
            else => try req.transferRead(buffer),
        };
        if (out_index > 0) return out_index;

        while (!req.response.parser.state.isContent()) { // read trailing headers
            try req.connection.?.fill();

            const nchecked = try req.response.parser.checkCompleteHead(req.connection.?.peek());
            req.connection.?.drop(@intCast(nchecked));
        }

        return 0;
    }

    /// Reads data from the response body. Must be called after `wait`.
    pub fn readAll(req: *Request, buffer: []u8) !usize {
        var index: usize = 0;
        while (index < buffer.len) {
            const amt = try read(req, buffer[index..]);
            if (amt == 0) break;
            index += amt;
        }
        return index;
    }

    pub const WriteError = Connection.WriteError || error{ NotWriteable, MessageTooLong };

    pub const Writer = std.io.Writer(*Request, WriteError, write);

    pub fn writer(req: *Request) Writer {
        return .{ .context = req };
    }

    /// Write `bytes` to the server. The `transfer_encoding` field determines how data will be sent.
    /// Must be called after `send` and before `finish`.
    pub fn write(req: *Request, bytes: []const u8) WriteError!usize {
        switch (req.transfer_encoding) {
            .chunked => {
                if (bytes.len > 0) {
                    try req.connection.?.writer().print("{x}\r\n", .{bytes.len});
                    try req.connection.?.writer().writeAll(bytes);
                    try req.connection.?.writer().writeAll("\r\n");
                }

                return bytes.len;
            },
            .content_length => |*len| {
                if (len.* < bytes.len) return error.MessageTooLong;

                const amt = try req.connection.?.write(bytes);
                len.* -= amt;
                return amt;
            },
            .none => return error.NotWriteable,
        }
    }

    /// Write `bytes` to the server. The `transfer_encoding` field determines how data will be sent.
    /// Must be called after `send` and before `finish`.
    pub fn writeAll(req: *Request, bytes: []const u8) WriteError!void {
        var index: usize = 0;
        while (index < bytes.len) {
            index += try write(req, bytes[index..]);
        }
    }

    pub const FinishError = WriteError || error{MessageNotCompleted};

    /// Finish the body of a request. This notifies the server that you have no more data to send.
    /// Must be called after `send`.
    pub fn finish(req: *Request) FinishError!void {
        switch (req.transfer_encoding) {
            .chunked => try req.connection.?.writer().writeAll("0\r\n\r\n"),
            .content_length => |len| if (len != 0) return error.MessageNotCompleted,
            .none => {},
        }

        try req.connection.?.flush();
    }
};

pub const basic_authorization = struct {
    pub const max_user_len = 255;
    pub const max_password_len = 255;
    pub const max_value_len = valueLength(max_user_len, max_password_len);

    const prefix = "Basic ";

    pub fn valueLength(user_len: usize, password_len: usize) usize {
        return prefix.len + std.base64.standard.Encoder.calcSize(user_len + 1 + password_len);
    }

    pub fn valueLengthFromUri(uri: Uri) usize {
        var stream = std.io.countingWriter(std.io.null_writer);
        try stream.writer().print("{user}", .{uri.user orelse Uri.Component.empty});
        const user_len = stream.bytes_written;
        stream.bytes_written = 0;
        try stream.writer().print("{password}", .{uri.password orelse Uri.Component.empty});
        const password_len = stream.bytes_written;
        return valueLength(@intCast(user_len), @intCast(password_len));
    }

    pub fn value(uri: std.Uri, out: []u8) []u8 {
        var buf: [max_user_len + ":".len + max_password_len]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buf);
        stream.writer().print("{user}", .{uri.user orelse Uri.Component.empty}) catch
            unreachable;
        assert(stream.pos <= max_user_len);
        stream.writer().print(":{password}", .{uri.password orelse Uri.Component.empty}) catch
            unreachable;

        @memcpy(out[0..prefix.len], prefix);
        const base64 = std.base64.standard.Encoder.encode(out[prefix.len..], stream.getWritten());
        return out[0 .. prefix.len + base64.len];
    }

    pub fn writeTo(uri: std.Uri, w: anytype) !void {
        var buf: [max_user_len + ":".len + max_password_len]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buf);
        stream.writer().print("{user}", .{uri.user orelse Uri.Component.empty}) catch
            unreachable;
        assert(stream.pos <= max_user_len);
        stream.writer().print(":{password}", .{uri.password orelse Uri.Component.empty}) catch
            unreachable;

        try w.writeAll(prefix);
        try std.base64.standard.Encoder.encodeWriter(w, stream.getWritten());
    }
};

fn validateUri(uri: std.Uri, arena: Allocator) !struct { Connection.Protocol, Uri } {
    const protocol_map = std.StaticStringMap(Connection.Protocol).initComptime(.{
        .{ "http", .plain },
        .{ "ws", .plain },
        .{ "https", .tls },
        .{ "wss", .tls },
    });
    const protocol = protocol_map.get(uri.scheme) orelse return error.UnsupportedUriScheme;
    var valid_uri = uri;
    // The host is always going to be needed as a raw string for hostname resolution anyway.
    valid_uri.host = .{
        .raw = try (uri.host orelse return error.UriMissingHost).toRawMaybeAlloc(arena),
    };
    return .{ protocol, valid_uri };
}

fn uriPort(uri: Uri, protocol: Connection.Protocol) u16 {
    return uri.port orelse switch (protocol) {
        .plain => 80,
        .tls => 443,
    };
}

/// A set of linked lists of connections that can be reused.
pub const ConnectionPool = struct {
    /// Open connections that are currently in use.
    used: Queue = .{},
    /// Open connections that are not currently in use.
    free: Queue = .{},
    free_len: usize = 0,
    free_size: usize = 32,

    /// The criteria for a connection to be considered a match.
    pub const Criteria = struct {
        host: []const u8,
        port: u16,
        protocol: Connection.Protocol,
    };

    const Queue = std.DoublyLinkedList(Connection);
    pub const Node = Queue.Node;

    /// Finds and acquires a connection from the connection pool matching the criteria. This function is threadsafe.
    /// If no connection is found, null is returned.
    pub fn findConnection(pool: *ConnectionPool, criteria: Criteria) ?*Connection {
        var next = pool.free.last;
        while (next) |node| : (next = node.prev) {
            if (node.data.protocol != criteria.protocol) continue;
            if (node.data.port != criteria.port) continue;

            // Domain names are case-insensitive (RFC 5890, Section 2.3.2.4)
            if (!std.ascii.eqlIgnoreCase(node.data.host, criteria.host)) continue;

            pool.acquireUnsafe(node);
            return &node.data;
        }

        return null;
    }

    /// Acquires an existing connection from the connection pool. This function is not threadsafe.
    pub fn acquireUnsafe(pool: *ConnectionPool, node: *Node) void {
        pool.free.remove(node);
        pool.free_len -= 1;

        pool.used.append(node);
    }

    /// Acquires an existing connection from the connection pool. This function is threadsafe.
    pub fn acquire(pool: *ConnectionPool, node: *Node) void {
        return pool.acquireUnsafe(node);
    }

    /// Tries to release a connection back to the connection pool. This function is threadsafe.
    /// If the connection is marked as closing, it will be closed instead.
    ///
    /// The allocator must be the owner of all nodes in this pool.
    /// The allocator must be the owner of all resources associated with the connection.
    pub fn release(pool: *ConnectionPool, allocator: Allocator, connection: *Connection) void {
        const node: *Node = @fieldParentPtr("data", connection);

        pool.used.remove(node);

        if (node.data.closing or pool.free_size == 0) {
            node.data.close();
            return allocator.destroy(node);
        }

        if (pool.free_len >= pool.free_size) {
            const popped = pool.free.popFirst() orelse unreachable;
            pool.free_len -= 1;

            popped.data.close();
            allocator.destroy(popped);
        }

        if (node.data.proxied) {
            pool.free.prepend(node); // proxied connections go to the end of the queue, always try direct connections first
        } else {
            pool.free.append(node);
        }

        pool.free_len += 1;
    }

    /// Adds a newly created node to the pool of used connections. This function is threadsafe.
    pub fn addUsed(pool: *ConnectionPool, node: *Node) void {
        pool.used.append(node);
    }

    /// Resizes the connection pool. This function is threadsafe.
    ///
    /// If the new size is smaller than the current size, then idle connections will be closed until the pool is the new size.
    pub fn resize(pool: *ConnectionPool, allocator: Allocator, new_size: usize) void {
        const next = pool.free.first;
        _ = next;
        while (pool.free_len > new_size) {
            const popped = pool.free.popFirst() orelse unreachable;
            pool.free_len -= 1;

            popped.data.close(allocator);
            allocator.destroy(popped);
        }

        pool.free_size = new_size;
    }

    /// Frees the connection pool and closes all connections within. This function is threadsafe.
    ///
    /// All future operations on the connection pool will deadlock.
    pub fn deinit(pool: *ConnectionPool, allocator: Allocator) void {
        var next = pool.free.first;
        while (next) |node| {
            defer allocator.destroy(node);
            next = node.next;

            node.data.close();
        }

        next = pool.used.first;
        while (next) |node| {
            defer allocator.destroy(node);
            next = node.next;

            node.data.close();
        }

        pool.* = undefined;
    }
};

/// Open a connection to the host specified by `uri` and prepare to send a HTTP request.
///
///
/// `uri` must remain alive during the entire request.
///
/// The caller is responsible for calling `deinit()` on the `Request`.
/// This function is threadsafe.
///
/// Asserts that "\r\n" does not occur in any header name or value.
pub fn open(
    client: *Client,
    arena: Allocator,
    method: http.Method,
    uri: Uri,
    options: RequestOptions,
    ptr: ?*anyopaque,
    callback: *const fn (*Request) anyerror!void,
    ring: *io.Runtime,
) !*Request {
    for (options.extra_headers) |header| {
        assert(header.name.len != 0);
        assert(std.mem.indexOfScalar(u8, header.name, ':') == null);
        assert(std.mem.indexOfPosLinear(u8, header.name, 0, "\r\n") == null);
        assert(std.mem.indexOfPosLinear(u8, header.value, 0, "\r\n") == null);
    }
    for (options.privileged_headers) |header| {
        assert(header.name.len != 0);
        assert(std.mem.indexOfPosLinear(u8, header.name, 0, "\r\n") == null);
        assert(std.mem.indexOfPosLinear(u8, header.value, 0, "\r\n") == null);
    }

    const req = try arena.create(Request);

    const protocol, const valid_uri = try validateUri(uri, arena);

    const connection = options.connection orelse
        try client.connect(valid_uri.host.?.raw, uriPort(valid_uri, protocol), protocol, ring);

    connection.request = req;

    req.* = .{
        .arena = arena,
        .ptr = ptr,
        .callback = callback,
        .uri = valid_uri,
        .client = client,
        .connection = connection,
        .keep_alive = options.keep_alive,
        .method = method,
        .version = options.version,
        .transfer_encoding = .none,
        .redirect_behavior = options.redirect_behavior,
        .handle_continue = options.handle_continue,
        .response = .{ .bytes = .empty },
        .headers = options.headers,
        .extra_headers = options.extra_headers,
        .privileged_headers = options.privileged_headers,
    };
    errdefer req.deinit();

    if (connection.state == .idle) {
        connection.state = .send_request;
        try Connection.onTaskCompletion(connection, ring, 0, .noop);
        // Rearm our recv task
        _ = try ring.recv(
            connection.fd,
            &connection.recv_buf,
            connection,
            0,
            Connection.onTaskCompletion,
        );
    }

    return req;
}

pub const RequestOptions = struct {
    version: http.Version = .@"HTTP/1.1",

    /// Automatically ignore 100 Continue responses. This assumes you don't
    /// care, and will have sent the body before you wait for the response.
    ///
    /// If this is not the case AND you know the server will send a 100
    /// Continue, set this to false and wait for a response before sending the
    /// body. If you wait AND the server does not send a 100 Continue before
    /// you finish the request, then the request *will* deadlock.
    handle_continue: bool = true,

    /// If false, close the connection after the one request. If true,
    /// participate in the client connection pool.
    keep_alive: bool = true,

    /// This field specifies whether to automatically follow redirects, and if
    /// so, how many redirects to follow before returning an error.
    ///
    /// This will only follow redirects for repeatable requests (ie. with no
    /// payload or the server has acknowledged the payload).
    redirect_behavior: Request.RedirectBehavior = @enumFromInt(3),

    /// Externally-owned memory used to store the server's entire HTTP header.
    /// `error.HttpHeadersOversize` is returned from read() when a
    /// client sends too many bytes of HTTP headers.
    server_header_buffer: []u8,

    /// Must be an already acquired connection.
    connection: ?*Connection = null,

    /// Standard headers that have default, but overridable, behavior.
    headers: Request.Headers = .{},
    /// These headers are kept including when following a redirect to a
    /// different domain.
    /// Externally-owned; must outlive the Request.
    extra_headers: []const http.Header = &.{},
    /// These headers are stripped when following a redirect to a different
    /// domain.
    /// Externally-owned; must outlive the Request.
    privileged_headers: []const http.Header = &.{},
};

pub const Response = struct {
    /// The raw bytes of the response
    bytes: std.ArrayListUnmanaged(u8) = .empty,

    // add the slice to the internal buffer
    pub fn appendSlice(self: *Response, gpa: Allocator, bytes: []const u8) !void {
        try self.bytes.appendSlice(gpa, bytes);
    }

    /// iterates over headers and trailers
    pub fn headerIterator(self: Response) HeaderIterator {
        assert(self.receivedHeader());
        return .init(self.bytes.items);
    }

    pub fn headLen(self: Response) ?usize {
        const idx = std.mem.indexOf(
            u8,
            self.bytes.items,
            "\r\n" ++ "\r\n",
        ) orelse return null;

        return idx + 4;
    }

    /// Returns true if we have received the full header
    pub fn receivedHeader(self: Response) bool {
        return self.headLen() != null;
    }

    /// Returns the body of the request. Null indicates there is a body and we haven't read the
    /// entirety of it. An empty string indicates the request has no body and is not expecting one
    pub fn body(self: Response) ?[]const u8 {
        const head_len = self.headLen() orelse return null;

        const cl = self.contentLength() orelse {
            // TODO: we need to also check for chunked transfer encoding
            return "";
        };

        if (cl + head_len == self.bytes.items.len) return self.bytes.items[head_len..];

        return null;
    }

    pub fn getHeader(self: Response, key: []const u8) ?[]const u8 {
        var iter = self.headerIterator();
        while (iter.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, key)) return header.value;
        }
        return null;
    }

    pub fn contentLength(self: Response) ?u64 {
        const value = self.getHeader("Content-Length") orelse return null;
        return std.fmt.parseUnsigned(u64, value, 10) catch @panic("TODO: bad content length");
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

test "client: handshake" {
    var ring = try io.Runtime.init(std.testing.allocator, 8);
    defer ring.deinit();

    var client: Client = try .init(std.testing.allocator);
    defer client.deinit();

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();

    const req = try client.fetch(arena.allocator(), &ring, .{
        .location = .{ .url = "https://timculverhouse.com" },
    }, null, testFetchCallback);
    _ = req;

    // var conn: Connection = undefined;
    // var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // defer arena.deinit();
    //
    // const req = Request.init(arena.allocator(), .GET, "https://timculverhouse.com/");
    // try conn.init(arena.allocator(), client.cert, &ring, req);

    try ring.run(.until_done);
}

fn testFetchCallback(request: *Request) anyerror!void {
    log.err("headers={?s}", .{request.response.bytes.items[0..request.response.headLen().?]});
    log.err("body={?s}", .{request.response.body()});
}
