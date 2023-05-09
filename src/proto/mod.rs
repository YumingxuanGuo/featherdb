pub mod registration {
    tonic::include_proto!("registration");
}

pub mod session {
    tonic::include_proto!("session");
}

pub mod raft {
    tonic::include_proto!("raft");
}

pub mod raft_server {
    tonic::include_proto!("raft_server");
}