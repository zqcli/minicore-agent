use std::path::{Path, PathBuf};

use minicore_agent::SessionId;

pub struct Fixture(pub PathBuf);

impl Fixture {
    pub fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "minicore-agent-{label}-{}",
            SessionId::new().unwrap()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
