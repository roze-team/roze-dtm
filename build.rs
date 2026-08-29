fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    println!("cargo:rerun-if-changed=proto/dtmgimp.proto");
    println!("cargo:rerun-if-changed=proto/workflow_callback_test.proto");
    roze_grpc::build::compile(
        &["proto/dtmgimp.proto", "proto/workflow_callback_test.proto"],
        &["proto"],
    )?;
    Ok(())
}
