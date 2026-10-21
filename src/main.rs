#![forbid(unsafe_code)]

fn main() {
    std::process::exit(minicore_agent::cli::run(std::env::args().skip(1)));
}
