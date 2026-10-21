//! UTF-8 helpers for byte-bounded text.

/// The longest prefix of `text` within `max_bytes`, and whether it was cut.
pub(crate) fn truncate(text: &str, max_bytes: usize) -> (&str, bool) {
    let end = text.floor_char_boundary(max_bytes);
    (&text[..end], end < text.len())
}
