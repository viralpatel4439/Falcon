#![forbid(unsafe_code)]

mod rest;
mod server;
mod state;
mod ws;

pub use server::{router, serve, serve_with_shutdown};
pub use state::AppState;