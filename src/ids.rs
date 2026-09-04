use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

const SESSION_ID_PREFIX: &str = "ses_";

/// Errors creating or parsing an agent-owned [`SessionId`].
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SessionIdError {
    #[error("session ID has an invalid prefix")]
    InvalidPrefix,
    #[error("session ID has an invalid length")]
    InvalidLength,
    #[error("session ID contains non-canonical hexadecimal")]
    InvalidHex,
    #[error("session ID payload must not be all zero")]
    ZeroPayload,
    #[error("cryptographic random source unavailable")]
    EntropyUnavailable,
}

/// Agent-owned session identifier rendered as `ses_<32 lower-case hex>`.
///
/// The string format is stable (URLs, RPC parameters, directory names, and
/// user bookmarks keep working), but this does not imply any on-disk
/// compatibility with older session stores.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionId([u8; 16]);

impl SessionId {
    pub fn new() -> Result<Self, SessionIdError> {
        loop {
            let mut bytes = [0; 16];
            getrandom::fill(&mut bytes).map_err(|_| SessionIdError::EntropyUnavailable)?;
            if bytes.iter().any(|byte| *byte != 0) {
                return Ok(Self(bytes));
            }
        }
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl FromStr for SessionId {
    type Err = SessionIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let payload = value
            .strip_prefix(SESSION_ID_PREFIX)
            .ok_or(SessionIdError::InvalidPrefix)?;
        if payload.len() != 32 {
            return Err(SessionIdError::InvalidLength);
        }
        let mut bytes = [0; 16];
        for (index, pair) in payload.as_bytes().chunks_exact(2).enumerate() {
            let high = decode_lower_hex(pair[0]).ok_or(SessionIdError::InvalidHex)?;
            let low = decode_lower_hex(pair[1]).ok_or(SessionIdError::InvalidHex)?;
            bytes[index] = (high << 4) | low;
        }
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(SessionIdError::ZeroPayload);
        }
        Ok(Self(bytes))
    }
}

fn decode_lower_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn encode_session_id(bytes: &[u8; 16]) -> String {
    let mut value = String::with_capacity(SESSION_ID_PREFIX.len() + 32);
    value.push_str(SESSION_ID_PREFIX);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&encode_session_id(&self.0))
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Serialize for SessionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_session_id(&self.0))
    }
}

impl<'de> Deserialize<'de> for SessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}
