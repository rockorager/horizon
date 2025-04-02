const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const horizon_module = b.addModule("horizon", .{
        .root_source_file = b.path("src/horizon.zig"),
        .target = target,
        .optimize = optimize,
    });

    const zeit_dep = b.dependency("zeit", .{ .target = target, .optimize = optimize });
    horizon_module.addImport("zeit", zeit_dep.module("zeit"));

    const unit_tests = b.addTest(.{ .root_module = horizon_module });

    const run_unit_tests = b.addRunArtifact(unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    {
        // Horizon bench
        const hz_exe = b.addExecutable(.{
            .name = "horizon",
            .root_source_file = b.path("bench/horizon.zig"),
            .target = target,
            .optimize = optimize,
        });
        hz_exe.root_module.addImport("horizon", horizon_module);
        b.installArtifact(hz_exe);

        const run_cmd = b.addRunArtifact(hz_exe);

        run_cmd.step.dependOn(b.getInstallStep());

        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        const run_step = b.step("bench-horizon", "Run horizon bench server");
        run_step.dependOn(&run_cmd.step);
    }
    {
        // Go bench
        const run_cmd = b.addSystemCommand(&.{ "go", "run", "bench/main.go" });

        run_cmd.step.dependOn(b.getInstallStep());

        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        const run_step = b.step("bench-go", "Run golang bench server");
        run_step.dependOn(&run_cmd.step);
    }

    {
        // http.zig bench
        const httpz_exe = b.addExecutable(.{
            .name = "http.zig",
            .root_source_file = b.path("bench/http.zig"),
            .target = target,
            .optimize = optimize,
        });
        const httpz = b.lazyDependency("httpz", .{ .target = target, .optimize = optimize });
        httpz_exe.root_module.addImport("httpz", httpz.?.module("httpz"));
        b.installArtifact(httpz_exe);

        const run_cmd = b.addRunArtifact(httpz_exe);

        run_cmd.step.dependOn(b.getInstallStep());

        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        const run_step = b.step("bench-httpz", "Run http.zig bench server");
        run_step.dependOn(&run_cmd.step);
    }
}
