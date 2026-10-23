//! Output cutting shared by `bash` and `read`: 2000 lines or 50 KiB, whichever
//! is hit first.

/// The end of a text that exceeded its limits.
pub(super) struct Tail<'a> {
    pub(super) text: &'a str,
    pub(super) total_lines: usize,
    pub(super) kept_lines: usize,
    pub(super) by_bytes: bool,
    /// `text` is only the end of the last line, which alone exceeds the byte limit.
    pub(super) partial: bool,
    pub(super) last_line_bytes: usize,
}

/// Keeps whole lines from the end of `text`, or `None` when nothing is cut.
pub(super) fn truncate_tail(text: &str, max_lines: usize, max_bytes: usize) -> Option<Tail<'_>> {
    let total_lines = text.split_terminator('\n').count();
    if total_lines <= max_lines && text.len() <= max_bytes {
        return None;
    }
    let body = text.strip_suffix('\n').unwrap_or(text);
    let mut kept_lines = 0;
    let mut kept_bytes = 0;
    let mut by_bytes = false;
    let mut partial = false;
    let mut last_line_bytes = 0;
    for line in body.rsplit('\n') {
        if kept_lines == 0 {
            last_line_bytes = line.len();
        }
        if kept_lines == max_lines {
            break;
        }
        let cost = line.len() + usize::from(kept_lines > 0);
        if kept_bytes + cost > max_bytes {
            by_bytes = true;
            if kept_lines == 0 {
                kept_lines = 1;
                kept_bytes = line.len() - line.ceil_char_boundary(line.len() - max_bytes);
                partial = true;
            }
            break;
        }
        kept_lines += 1;
        kept_bytes += cost;
    }
    if kept_lines == total_lines && !partial {
        return None;
    }
    Some(Tail {
        text: &body[body.len() - kept_bytes..],
        total_lines,
        kept_lines,
        by_bytes,
        partial,
        last_line_bytes,
    })
}

/// Splits `total` between two parts so that neither gets less than half unless
/// it needs less.
pub(super) fn share(first: usize, second: usize, total: usize) -> (usize, usize) {
    let first_budget = if first <= total / 2 {
        first
    } else {
        total - second.min(total / 2)
    };
    (first_budget, total - first_budget)
}

/// A size as `512B`, `50KB` or `1.5MB`.
pub(super) fn format_size(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    let one_decimal = |value: f64| format!("{value:.1}").trim_end_matches(".0").to_owned();
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{}KB", one_decimal(bytes as f64 / KB))
    } else {
        format!("{}MB", one_decimal(bytes as f64 / (KB * KB)))
    }
}

#[cfg(test)]
mod tests;
