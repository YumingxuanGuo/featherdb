fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["src/proto/hello_world.proto"], &["proto"])
        .unwrap();

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["src/proto/database.proto"], &["proto"])
        .unwrap();
}