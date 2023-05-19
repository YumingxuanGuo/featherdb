pub mod registration {
    tonic::include_proto!("registration");
}

pub mod session {
    tonic::include_proto!("session");
}

pub mod raft {
    tonic::include_proto!("raft");
}

pub mod featherkv {
    tonic::include_proto!("featherkv");
    pub use feather_kv_server::{FeatherKv, FeatherKvServer};
    pub use feather_kv_client::FeatherKvClient;
}

pub mod featherdb {
    tonic::include_proto!("featherdb");
    pub use feather_db_server::{FeatherDb, FeatherDbServer};
    pub use feather_db_client::FeatherDbClient;
}