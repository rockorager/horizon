const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const horizon_module = b.addModule("horizon", .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const ourio_dep = b.dependency("ourio", .{ .target = target, .optimize = optimize });
    const ourio_mod = ourio_dep.module("ourio");

    horizon_module.addImport("ourio", ourio_mod);

    const zeit_dep = b.dependency("zeit", .{ .target = target, .optimize = optimize });
    horizon_module.addImport("zeit", zeit_dep.module("zeit"));

    const tls_dep = b.dependency("tls", .{ .target = target, .optimize = optimize });
    horizon_module.addImport("tls", tls_dep.module("tls"));

    const unit_tests = b.addTest(.{ .root_module = horizon_module });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    run_unit_tests.skip_foreign_checks = true;
    const install_hz_tests = b.addInstallBinFile(unit_tests.getEmittedBin(), "test");
    const hz_test_step = b.step("test", "Run horizon unit tests");
    hz_test_step.dependOn(&run_unit_tests.step);
    hz_test_step.dependOn(&install_hz_tests.step);

    // Bench run command
    {
        const bench_server = b.option(BenchServer, "bench-server", "Run a bench server") orelse .horizon;
        const run_cmd: *std.Build.Step.Run = switch (bench_server) {
            .go => b.addSystemCommand(&.{ "go", "run", "bench/main.go" }),

            .horizon => blk: {
                // Horizon bench
                const hz_exe = b.addExecutable(.{
                    .name = "horizon",
                    .root_source_file = b.path("bench/horizon.zig"),
                    .target = target,
                    .optimize = optimize,
                });
                hz_exe.root_module.addImport("horizon", horizon_module);
                hz_exe.root_module.addImport("ourio", ourio_mod);
                b.installArtifact(hz_exe);

                break :blk b.addRunArtifact(hz_exe);
            },

            .httpz => blk: {
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

                break :blk b.addRunArtifact(httpz_exe);
            },

            .zzz => blk: {
                // zzz bench
                const zzz_exe = b.addExecutable(.{
                    .name = "zzz",
                    .root_source_file = b.path("bench/zzz.zig"),
                    .target = target,
                    .optimize = optimize,
                });
                const zzz = b.lazyDependency("zzz", .{ .target = target, .optimize = optimize });
                zzz_exe.root_module.addImport("zzz", zzz.?.module("zzz"));
                b.installArtifact(zzz_exe);

                break :blk b.addRunArtifact(zzz_exe);
            },
        };

        run_cmd.step.dependOn(b.getInstallStep());

        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        const run_step = b.step("bench", "Run a bench server");
        run_step.dependOn(&run_cmd.step);
    }

    {
        const Examples = enum {
            basic,
            kqueue,
        };

        // Example run commands
        const example = b.option(Examples, "example", "Run an example server") orelse .basic;
        const example_exe = b.addExecutable(.{
            .name = @tagName(example),
            .root_source_file = b.path(b.fmt("examples/{s}.zig", .{@tagName(example)})),
            .target = target,
            .optimize = optimize,
        });
        example_exe.root_module.addImport("horizon", horizon_module);
        example_exe.root_module.addImport("ourio", ourio_mod);
        b.installArtifact(example_exe);

        const run_cmd = b.addRunArtifact(example_exe);

        run_cmd.step.dependOn(b.getInstallStep());

        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        const run_step = b.step("example", "Run an example server");
        run_step.dependOn(&run_cmd.step);
    }
}

const BenchServer = enum {
    go,
    horizon,
    httpz,
    zzz,
};
