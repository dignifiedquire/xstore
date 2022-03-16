use std::{env, path::PathBuf, process::Command};

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let lib = "xstore";
    let out_file = PathBuf::from(&out_dir).join(format!("lib{}.a", lib));

    Command::new("go")
        .args(&[
            "build",
            "-buildmode=c-archive",
            "-o",
            out_file.to_str().unwrap(),
            "./",
        ])
        .status()
        .unwrap();

    println!("cargo:rustc-link-search=native={}", out_dir);
    println!("cargo:rustc-link-lib=static={}", lib);
    println!("cargo:rerun-if-changed=src/*.go");
}
