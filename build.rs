fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["src/proto/registration.proto"], &["proto"])
        .unwrap();

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["src/proto/session.proto"], &["proto"])
        .unwrap();

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["src/proto/raft.proto"], &["proto"])
        .unwrap();

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["src/proto/raft_server.proto"], &["proto"])
        .unwrap();
}