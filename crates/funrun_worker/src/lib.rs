//! Stateless remote function runner worker. It has no database: reads and
//! action callbacks go back to the conductor's `function_host` over gRPC.

pub mod host_client;
pub mod load;
mod metrics;
