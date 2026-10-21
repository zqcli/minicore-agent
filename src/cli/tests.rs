use super::*;

#[test]
fn the_exit_code_reports_bad_arguments_and_a_missing_config() {
    let args = |list: &[&str]| list.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    assert_eq!(run(args(&["--version"])), 0);
    assert_eq!(run(args(&["--stdio"])), 1);
    assert_eq!(
        run(args(&["--config", "/nonexistent/agent.toml", "--stdio"])),
        1
    );
}
