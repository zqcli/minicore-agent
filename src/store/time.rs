//! RFC3339 timestamp validation and formatting.
use super::StoreError;

const MAX_TIMESTAMP_BYTES: usize = 64;

pub(super) fn valid_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() > MAX_TIMESTAMP_BYTES || bytes.len() < 20 {
        return false;
    }
    if bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
    }
    let digits = |range: std::ops::Range<usize>| {
        bytes
            .get(range)
            .is_some_and(|part| part.iter().all(u8::is_ascii_digit))
    };
    if !digits(0..4)
        || !digits(5..7)
        || !digits(8..10)
        || !digits(11..13)
        || !digits(14..16)
        || !digits(17..19)
    {
        return false;
    }
    let number = |range: std::ops::Range<usize>| {
        std::str::from_utf8(&bytes[range])
            .ok()
            .and_then(|part| part.parse::<u32>().ok())
    };
    let Some(year) = number(0..4) else {
        return false;
    };
    let Some(month) = number(5..7) else {
        return false;
    };
    let Some(day) = number(8..10) else {
        return false;
    };
    let Some(hour) = number(11..13) else {
        return false;
    };
    let Some(minute) = number(14..16) else {
        return false;
    };
    let Some(second) = number(17..19) else {
        return false;
    };
    if year == 0
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return false;
    }

    let mut index = 19;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == start {
            return false;
        }
    }
    match bytes.get(index) {
        Some(b'Z') => index + 1 == bytes.len(),
        Some(b'+' | b'-') => {
            index += 1;
            if index + 5 != bytes.len()
                || bytes.get(index + 2) != Some(&b':')
                || !digits(index..index + 2)
                || !digits(index + 3..index + 5)
            {
                return false;
            }
            let offset_hour = number(index..index + 2).unwrap_or(24);
            let offset_minute = number(index + 3..index + 5).unwrap_or(60);
            offset_hour <= 23 && offset_minute <= 59
        }
        _ => false,
    }
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        _ => 0,
    }
}

pub(crate) fn utc_timestamp() -> Result<String, StoreError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| StoreError::Unavailable)?;
    Ok(format_rfc3339_millis(
        duration.as_secs(),
        duration.subsec_millis(),
    ))
}

fn format_rfc3339_millis(total_seconds: u64, millis: u32) -> String {
    const SECS_PER_DAY: i64 = 86_400;
    let days = i64::try_from(total_seconds)
        .unwrap_or(0)
        .div_euclid(SECS_PER_DAY);
    let seconds_of_day = i64::try_from(total_seconds)
        .unwrap_or(0)
        .rem_euclid(SECS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month as u32, day as u32)
}
