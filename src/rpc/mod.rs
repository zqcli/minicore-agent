mod protocol;
mod server;

pub(crate) use server::MAX_RPC_FRAME_BYTES;
pub(crate) use server::WRITER_BUFFER_BYTES;
pub use server::run_stdio;
