const std = @import("std");
const zeit = @import("zeit");

const default_logger = Logger{ .prefix = "" };

var isatty: bool = false;
var log_level: std.log.Level = .debug;

/// Call init if you want to check for a tty. By default, we assume output is not a tty. If it is a
/// tty, we will use some color
pub fn init(level: std.log.Level) void {
    isatty = std.io.getStdErr().isTty();
    log_level = level;
}

pub fn err(comptime fmt: []const u8, args: anytype) void {
    return default_logger.log(.err, fmt, args) catch return;
}

pub fn warn(comptime fmt: []const u8, args: anytype) void {
    return default_logger.log(.warn, fmt, args) catch return;
}

pub fn info(comptime fmt: []const u8, args: anytype) void {
    return default_logger.log(.info, fmt, args) catch return;
}

pub fn debug(comptime fmt: []const u8, args: anytype) void {
    return default_logger.log(.debug, fmt, args) catch return;
}

pub const TimeFormat = union(enum) {
    strftime: []const u8,
    gofmt: []const u8,
};

pub const Logger = struct {
    prefix: []const u8,

    fn log(self: Logger, level: std.log.Level, comptime fmt: []const u8, args: anytype) !void {
        if (@intFromEnum(level) > @intFromEnum(log_level)) return;
        const stderr = std.io.getStdErr().writer();
        var bw = std.io.bufferedWriter(stderr);
        const writer = bw.writer();

        if (isatty) try writer.writeAll("\x1b[2m");
        const now = (zeit.instant(.{}) catch unreachable).time();
        try writer.print(
            "{d}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z",
            .{
                now.year,
                @intFromEnum(now.month),
                now.day,
                now.hour,
                now.minute,
                now.second,
                now.millisecond,
            },
        );
        if (isatty) try writer.writeAll("\x1b[m");

        const level_txt: []const u8 = if (isatty)
            switch (level) {
                .err => "\x1b[31mERR\x1b[m",
                .info => "\x1b[34mINF\x1b[m",
                .warn => "\x1b[33mWRN\x1b[m",
                .debug => "\x1b[35mDBG\x1b[m",
            }
        else switch (level) {
            .err => "ERR",
            .info => "INF",
            .warn => "WRN",
            .debug => "DBG",
        };

        std.debug.lockStdErr();
        defer std.debug.unlockStdErr();
        nosuspend {
            writer.print(" {s} {s}", .{ level_txt, self.prefix }) catch return;
            writer.print(fmt ++ "\n", args) catch return;
            bw.flush() catch return;
        }
    }

    pub fn err(self: Logger, comptime fmt: []const u8, args: anytype) void {
        return self.log(.err, fmt, args) catch return;
    }

    pub fn warn(self: Logger, comptime fmt: []const u8, args: anytype) void {
        return self.log(.warn, fmt, args) catch return;
    }

    pub fn info(self: Logger, comptime fmt: []const u8, args: anytype) void {
        return self.log(.info, fmt, args) catch return;
    }

    pub fn debug(self: Logger, comptime fmt: []const u8, args: anytype) void {
        return self.log(.debug, fmt, args) catch return;
    }
};
