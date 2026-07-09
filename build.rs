// build.rs — embeds git identity at compile time
use std::process::Command;

fn main() {
    let sha = Command::new("git").args(["rev-parse", "--short", "HEAD"])
        .output().ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    let dirty = Command::new("git").args(["status", "--porcelain"])
        .output().map(|o| !o.stdout.is_empty()).unwrap_or(false);
    println!("cargo:rustc-env=GIT_SHA={}{}", sha, if dirty { "-dirty" } else { "" });
    println!("cargo:rerun-if-changed=.git/HEAD");
    // HEAD only changes on branch switch; the ref file changes on commit.
    if let Ok(head) = std::fs::read_to_string(".git/HEAD") {
        if let Some(reference) = head.strip_prefix("ref: ") {
            println!("cargo:rerun-if-changed=.git/{}", reference.trim());
        }
    }
}