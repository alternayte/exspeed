use std::process::Command;

#[test]
fn cli_prints_help_for_every_subcommand() {
    // Build in debug mode once at test start. Rely on cargo's cache.
    let out = Command::new(env!("CARGO_BIN_EXE_exspeed-bench"))
        .arg("--help")
        .output()
        .expect("run --help");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let help = String::from_utf8(out.stdout).unwrap();
    for cmd in ["publish", "latency", "fanout", "exql", "all", "render"] {
        assert!(help.contains(cmd), "missing subcommand {cmd} in --help output:\n{help}");
    }
}
