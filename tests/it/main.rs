// tests/support is shared with the separate rpc_soak target.
#[path = "../support/mod.rs"]
pub mod support;

#[cfg(feature = "e2e-observe")]
mod e2e_input;
mod e2e_observe;
mod legacy_closeout;
mod openai_rpc_process;
mod rpc_stdio;
mod shared_data_clients;
mod skeleton;
mod startup_errors;
mod tui_rpc_flow;
