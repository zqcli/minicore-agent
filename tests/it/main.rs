// Resolve the shared module tree from tests/support without path attributes.
pub mod support {
    include!("../support/mod.rs");
}

mod legacy_closeout;
mod openai_rpc_process;
mod rpc_stdio;
mod shared_data_clients;
mod skeleton;
mod startup_errors;
mod tui_rpc_flow;
