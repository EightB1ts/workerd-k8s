# WARNING: THIS FILE IS AUTOGENERATED BY update-deps.py DO NOT EDIT

load("@//:build/http.bzl", "http_file")

TAG_NAME = "0.60.0"
URL = "https://github.com/bazelbuild/rules_rust/releases/download/0.60.0/cargo-bazel-x86_64-pc-windows-msvc.exe"
SHA256 = "882672c5e1c611a89d5a011f7e6276682dd1252d2e23ee1b36e13c60d2f3fea9"

def dep_cargo_bazel_win_x64():
    http_file(
        name = "cargo_bazel_win_x64",
        url = URL,
        executable = True,
        sha256 = SHA256,
        downloaded_file_path = "downloaded.exe",
    )
