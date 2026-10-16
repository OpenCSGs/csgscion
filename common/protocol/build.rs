//! 用纯 Rust 的 protox 编译 proto，不依赖系统 protoc。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files = [
        "scion/v1/common.proto",
        "scion/v1/server_info.proto",
        "scion/v1/object.proto",
    ];
    let fds = protox::compile(files, [&root])?;
    tonic_prost_build::configure().bytes(".").compile_fds(fds)?;
    println!("cargo:rerun-if-changed={}", root.display());
    Ok(())
}
