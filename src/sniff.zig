//! sniff implements https://mimesniff.spec.whatwg.org/, and is largely a rewrite of the golang
//! sniff function
const std = @import("std");

const assert = std.debug.assert;

/// We need at most 512 bytes to detect content type
const sniff_len = 512;

const whitespace = "\t\n\x0c\r ";

pub fn detectContentType(bytes: []const u8) []const u8 {
    const data: []const u8 = if (bytes.len > sniff_len) bytes[0..sniff_len] else bytes;

    const first_non_ws = std.mem.indexOfNone(u8, data, whitespace) orelse 0;

    for (sigs) |pattern| {
        if (pattern.match(data, first_non_ws)) |mime| return mime;
    }

    // Use the same fallback as go
    return "application/octet-stream";
}

const sigs = [_]Signature{
    .{ .html = .init("<!DOCTYPE HTML") },
    .{ .html = .init("<HTML") },
    .{ .html = .init("<HEAD") },
    .{ .html = .init("<SCRIPT") },
    .{ .html = .init("<IFRAME") },
    .{ .html = .init("<H1") },
    .{ .html = .init("<DIV") },
    .{ .html = .init("<FONT") },
    .{ .html = .init("<TABLE") },
    .{ .html = .init("<A") },
    .{ .html = .init("<STYLE") },
    .{ .html = .init("<TITLE") },
    .{ .html = .init("<B") },
    .{ .html = .init("<BODY") },
    .{ .html = .init("<BR") },
    .{ .html = .init("<P") },
    .{ .html = .init("<!--") },

    .{ .trimmed_exact = .init("<?xml", "text/xml; charset=utf-8") },

    .{ .exact = .init("%PDF-", "application/pdf") },
    .{ .exact = .init("%!PS-Adobe-", "application/postscript") },

    // UTF BOMs
    .{ .masked = .{
        .mask = "\xFF\xFF\x00\x00",
        .pattern = "\xFE\xFF\x00\x00",
        .content_type = "text/plain; charset=utf16-be",
    } },
    .{ .masked = .{
        .mask = "\xFF\xFF\x00\x00",
        .pattern = "\xFF\xFE\x00\x00",
        .content_type = "text/plain; charset=utf16-le",
    } },
    .{ .masked = .{
        .mask = "\xFF\xFF\xFF\x00",
        .pattern = "\xEF\xBB\xBF\x00",
        .content_type = "text/plain; charset=utf-8",
    } },

    // Image types
    .{ .exact = .init("\x00\x00\x01\x00", "image/x-icon") },
    .{ .exact = .init("\x00\x00\x02\x00", "image/x-icon") },
    .{ .exact = .init("BM", "image/bmp") },
    .{ .exact = .init("GIF87a", "image/gif") },
    .{ .exact = .init("GIF89a", "image/gif") },
    .{ .masked = .{
        .mask = "\xFF\xFF\xFF\xFF\x00\x00\x00\x00\xFF\xFF\xFF\xFF\xFF\xFF",
        .pattern = "RIFF\x00\x00\x00\x00WEBPVP",
        .content_type = "image/webp",
    } },
    .{ .exact = .init("\x89PNG\x0D\x0A\x1A\x0A", "image/png") },
    .{ .exact = .init("\xFF\xD8\xFF", "image/jpeg") },

    // Audio and Video
    .{ .masked = .{
        .mask = "\xFF\xFF\xFF\xFF\x00\x00\x00\x00\xFF\xFF\xFF\xFF",
        .pattern = "FORM\x00\x00\x00\x00AIFF",
        .content_type = "audio/aiff",
    } },
    .{ .masked = .{
        .mask = "\xFF\xFF\xFF",
        .pattern = "ID3",
        .content_type = "audio/mpeg",
    } },
    .{ .masked = .{
        .mask = "\xFF\xFF\xFF\xFF\xFF",
        .pattern = "OggS\x00",
        .content_type = "application/ogg",
    } },
    .{ .masked = .{
        .mask = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
        .pattern = "MThd\x00\x00\x00\x06",
        .content_type = "audio/midi",
    } },
    .{ .masked = .{
        .mask = "\xFF\xFF\xFF\xFF\x00\x00\x00\x00\xFF\xFF\xFF\xFF",
        .pattern = "RIFF\x00\x00\x00\x00AVI ",
        .content_type = "video/avi",
    } },
    .{ .masked = .{
        .mask = "\xFF\xFF\xFF\xFF\x00\x00\x00\x00\xFF\xFF\xFF\xFF",
        .pattern = "RIFF\x00\x00\x00\x00WAVE",
        .content_type = "audio/wave",
    } },
    .{ .mp4 = .{} },
    .{ .exact = .init("\x1A\x45\xDF\xA3", "video/webm") },

    // Fonts
    .{ .masked = .{
        .mask = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xFF\xFF",
        .pattern = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00LP",
        .content_type = "application/vnd.ms-fontobject",
    } },
    .{ .exact = .init("\x00\x01\x00\x00", "font/ttf") },
    .{ .exact = .init("OTTO", "font/otf") },
    .{ .exact = .init("ttcf", "font/collection") },
    .{ .exact = .init("wOFF", "font/woff") },
    .{ .exact = .init("wOF2", "font/woff2") },

    // Archives
    .{ .exact = .init("\x1F\x8B\x08", "application/x-gzip") },
    .{ .exact = .init("PK\x03\x04", "application/zip") },
    .{ .exact = .init("Rar!\x1A\x07\x00", "application/x-rar-compressed") },
    .{ .exact = .init("Rar!\x1A\x07\x01\x00", "application/x-rar-compressed") },

    // Wasm
    .{ .exact = .init("\x00\x61\x73\x6D", "application/wasm") },

    .{ .text = .{} },
};

const Signature = union(enum) {
    html: HtmlSignature,
    trimmed_exact: TrimmedExactSignature,
    masked: MaskedSignature,
    exact: ExactSignature,
    text: TextSignature,
    mp4: Mp4Signature,

    fn match(self: Signature, input: []const u8, first_non_ws: usize) ?[]const u8 {
        switch (self) {
            inline else => |p| return p.match(input, first_non_ws),
        }
    }
};

const HtmlSignature = struct {
    pattern: []const u8,

    fn init(pattern: []const u8) HtmlSignature {
        return .{ .pattern = pattern };
    }

    fn match(self: HtmlSignature, input: []const u8, first_non_ws: usize) ?[]const u8 {
        // Trim whitespace
        const trimmed = input[first_non_ws..];

        // HTML has to end in a tag terminator, we don't include that in the pattern or mask
        if (trimmed.len < (self.pattern.len + 1)) return null;

        const data = trimmed[0..self.pattern.len];

        var buffer: [32]u8 = undefined;
        for (data, 0..) |d, i| {
            if (std.ascii.isAlphabetic(d))
                buffer[i] = d & 0xDF
            else
                buffer[i] = d;
        }

        const masked_data = buffer[0..data.len];

        if (!std.mem.eql(u8, self.pattern, masked_data)) return null;

        switch (trimmed[data.len]) {
            ' ', '>' => return "text/html; charset=utf-8",
            else => return null,
        }
    }
};

const MaskedSignature = struct {
    pattern: []const u8,
    mask: []const u8,
    content_type: []const u8,

    fn init(pattern: []const u8, mask: []const u8, ct: []const u8) MaskedSignature {
        return .{ .pattern = pattern, .mask = mask, .content_type = ct };
    }

    fn match(self: MaskedSignature, input: []const u8, _: usize) ?[]const u8 {
        if (input.len < self.pattern.len) return null;

        const data = input[0..self.pattern.len];

        var buffer: [64]u8 = undefined;
        for (data, self.mask, 0..) |d, m, i| {
            buffer[i] = d & m;
        }

        const masked_data = buffer[0..data.len];

        if (std.mem.eql(u8, self.pattern, masked_data)) return self.content_type;
        return null;
    }
};

const TrimmedExactSignature = struct {
    pattern: []const u8,
    content_type: []const u8,

    fn init(pattern: []const u8, ct: []const u8) TrimmedExactSignature {
        return .{ .pattern = pattern, .content_type = ct };
    }

    fn match(self: TrimmedExactSignature, input: []const u8, first_non_ws: usize) ?[]const u8 {
        const trimmed = input[first_non_ws..];
        if (trimmed.len < self.pattern.len) return null;

        const data = trimmed[0..self.pattern.len];
        if (std.mem.eql(u8, self.pattern, data)) return self.content_type;
        return null;
    }
};

const ExactSignature = struct {
    pattern: []const u8,
    content_type: []const u8,

    fn init(pattern: []const u8, ct: []const u8) ExactSignature {
        return .{ .pattern = pattern, .content_type = ct };
    }

    fn match(self: ExactSignature, input: []const u8, _: usize) ?[]const u8 {
        if (input.len < self.pattern.len) return null;

        if (std.mem.eql(u8, self.pattern, input[0..self.pattern.len])) return self.content_type;
        return null;
    }
};

const Mp4Signature = struct {
    fn match(_: Mp4Signature, input: []const u8, _: usize) ?[]const u8 {
        if (input.len < 12) return null;

        // Read the first 4 bytes as the box size
        const box_size = std.mem.readInt(u32, input[0..4], .big);

        // The input must be at least one box size, and box size must be divisible by 4
        if (input.len < box_size or box_size % 4 != 0) return null;

        // the next 4 bytes must be ftyp
        if (!std.mem.eql(u8, input[4..8], "ftyp")) return null;

        var st: usize = 16;
        while (st < box_size) : (st += 4) {
            // Ignores the four bytes that correspond to the version number of the "major brand".
            if (st == 12) continue;

            if (std.mem.eql(u8, input[st .. st + 3], "mp4")) return "video/mp4";
        }

        return null;
    }
};

const TextSignature = struct {
    fn match(_: TextSignature, input: []const u8, first_non_ws: usize) ?[]const u8 {
        const data = input[first_non_ws..];

        for (data) |b| {
            switch (b) {
                0x00...0x08,
                0x0B,
                0x0E...0x1A,
                0x1C...0x1F,
                => return null,
                else => continue,
            }
        }

        return "text/plain; charset=utf-8";
    }
};

test {
    const expectEqualStrings = std.testing.expectEqualStrings;

    try expectEqualStrings("text/html; charset=utf-8", detectContentType("<!doCtYpe hTml>"));
    try expectEqualStrings("text/html; charset=utf-8", detectContentType("<hTml>"));
    try expectEqualStrings("text/html; charset=utf-8", detectContentType("   <hTml    >"));

    try expectEqualStrings("text/xml; charset=utf-8", detectContentType("   <?xml    >"));

    try expectEqualStrings("application/pdf", detectContentType("%PDF- slkjs"));
    try expectEqualStrings("application/postscript", detectContentType("%!PS-Adobe- askdj"));

    try expectEqualStrings("text/plain; charset=utf-8", detectContentType("hello, world!"));
}
