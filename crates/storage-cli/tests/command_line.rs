//! `storage-cli --help`, `--version` and unknown commands.

use std::process::Command;

fn cli(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_storage-cli"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn help_prints_the_commands_and_succeeds() {
    for flag in ["--help", "-h"] {
        let out = cli(&[flag]);
        assert!(out.status.success(), "{flag}: {out:?}");
        let text = String::from_utf8(out.stdout).unwrap();
        assert!(
            text.contains("remote-health <addr>") && text.contains("restore-pitr"),
            "{text}"
        );
    }
}

#[test]
fn version_prints_the_version() {
    for flag in ["--version", "-V"] {
        let out = cli(&[flag]);
        assert!(out.status.success());
        assert_eq!(
            String::from_utf8(out.stdout).unwrap().trim(),
            format!("storage-cli {}", env!("CARGO_PKG_VERSION"))
        );
    }
}

#[test]
fn an_unknown_command_is_an_error_that_names_it() {
    let out = cli(&["remote-helth", "127.0.0.1:7878"]);
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("unknown command 'remote-helth'"), "{err}");
}
