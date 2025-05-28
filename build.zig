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

    const unit_tests = b.addTest(.{ .root_module = horizon_module });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    run_unit_tests.skip_foreign_checks = true;
    const install_hz_tests = b.addInstallBinFile(unit_tests.getEmittedBin(), "test");
    const hz_test_step = b.step("test", "Run horizon unit tests");
    hz_test_step.dependOn(&run_unit_tests.step);
    hz_test_step.dependOn(&install_hz_tests.step);

    {
        const Examples = enum {
            basic,
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
