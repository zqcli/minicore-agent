//! UTF-8 helpers for byte-bounded text.

/// The largest char boundary at or below `index` (`str::floor_char_boundary`
/// is newer than the MSRV).
pub(crate) fn floor_char_boundary(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// The longest prefix of `text` within `max_bytes`, and whether it was cut.
pub(crate) fn truncate(text: &str, max_bytes: usize) -> (&str, bool) {
    let end = floor_char_boundary(text, max_bytes);
    (&text[..end], end < text.len())
}
