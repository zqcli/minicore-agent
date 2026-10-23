use super::*;

#[test]
fn text_within_both_limits_is_not_cut() {
    assert!(truncate_tail("", 2, 10).is_none());
    assert!(truncate_tail("a\nb\n", 2, 4).is_none());
    assert!(truncate_tail("a\nb", 2, 3).is_none());
}

#[test]
fn the_line_limit_keeps_the_last_lines() {
    let tail = truncate_tail("a\nb\nc\nd\n", 2, 100).unwrap();
    assert_eq!(tail.text, "c\nd");
    assert_eq!((tail.total_lines, tail.kept_lines), (4, 2));
    assert!(!tail.by_bytes && !tail.partial);
}

#[test]
fn the_byte_limit_keeps_whole_lines_counting_separators() {
    let tail = truncate_tail("aaa\nbbb\nccc", 100, 7).unwrap();
    assert_eq!(tail.text, "bbb\nccc");
    assert!(tail.by_bytes && !tail.partial);
    let tail = truncate_tail("aaa\nbbb\nccc", 100, 6).unwrap();
    assert_eq!(tail.text, "ccc");
}

#[test]
fn a_last_line_over_the_byte_limit_keeps_its_end_on_a_character_boundary() {
    let tail = truncate_tail("short\n€€€€", 100, 7).unwrap();
    assert_eq!(tail.text, "€€");
    assert_eq!((tail.total_lines, tail.kept_lines), (2, 1));
    assert!(tail.by_bytes && tail.partial);
    assert_eq!(tail.last_line_bytes, 12);
}

#[test]
fn an_empty_last_line_counts() {
    let tail = truncate_tail("a\nb\n\n", 2, 100).unwrap();
    assert_eq!(tail.text, "b\n");
    assert_eq!(tail.total_lines, 3);
}

#[test]
fn a_part_is_never_starved_by_a_large_one() {
    assert_eq!(share(10, 90, 100), (10, 90));
    assert_eq!(share(90, 10, 100), (90, 10));
    assert_eq!(share(90, 90, 100), (50, 50));
    assert_eq!(share(0, 500, 100), (0, 100));
}

#[test]
fn sizes_are_short() {
    assert_eq!(format_size(512), "512B");
    assert_eq!(format_size(50 * 1024), "50KB");
    assert_eq!(format_size(1536), "1.5KB");
    assert_eq!(format_size(1024 * 1024), "1MB");
}
