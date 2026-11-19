//! Short time spans for `--since`, such as `3d` or `90m`.

use anyhow::{Result, bail};

const UNITS: [(char, i64); 5] = [
    ('s', 1),
    ('m', 60),
    ('h', 60 * 60),
    ('d', 24 * 60 * 60),
    ('w', 7 * 24 * 60 * 60),
];

/// Parse a span into seconds. One number and one unit, no spaces.
pub fn parse_span(text: &str) -> Result<i64> {
    let Some(unit) = text.chars().next_back() else {
        bail!("a span is a number and a unit, such as 3d");
    };
    let number = &text[..text.len() - unit.len_utf8()];

    if number.is_empty() {
        if unit.is_ascii_digit() {
            bail!("span {text:?} has no unit; write it as {text}d or 90m");
        }
        bail!("span {text:?} has no number; write it as 3{unit} or 90m");
    }
    if !number.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("span {text:?} must be a whole number followed by s, m, h, d or w");
    }

    let Some((_, seconds_per_unit)) = UNITS.iter().find(|(symbol, _)| *symbol == unit) else {
        bail!("span {text:?} has unit {unit:?}; use s, m, h, d or w");
    };

    let value: i64 = number
        .parse()
        .map_err(|_| anyhow::anyhow!("span {text:?} is too large"))?;
    value
        .checked_mul(*seconds_per_unit)
        .ok_or_else(|| anyhow::anyhow!("span {text:?} is too large"))
}

/// Write seconds back as the shortest exact span, such as `259200` to `3d`.
pub fn format_span(seconds: i64) -> String {
    for (symbol, size) in UNITS.iter().rev() {
        if seconds > 0 && seconds % size == 0 {
            return format!("{}{symbol}", seconds / size);
        }
    }
    format!("{seconds}s")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_unit() {
        assert_eq!(parse_span("45s").unwrap(), 45);
        assert_eq!(parse_span("90m").unwrap(), 5_400);
        assert_eq!(parse_span("12h").unwrap(), 43_200);
        assert_eq!(parse_span("3d").unwrap(), 259_200);
        assert_eq!(parse_span("2w").unwrap(), 1_209_600);
        assert_eq!(parse_span("0s").unwrap(), 0);
    }

    #[test]
    fn rejects_spans_without_one_number_and_one_unit() {
        for bad in ["", "3", "d", "3dd", "-1d", "3 d", "1.5h", "3D", " 3d"] {
            assert!(parse_span(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn names_the_part_that_is_missing() {
        let no_unit = parse_span("3").unwrap_err().to_string();
        assert!(no_unit.contains("no unit"), "{no_unit}");

        let no_number = parse_span("d").unwrap_err().to_string();
        assert!(no_number.contains("no number"), "{no_number}");
    }

    #[test]
    fn rejects_an_overflow() {
        assert!(parse_span("99999999999999999999d").is_err());
    }

    #[test]
    fn format_is_the_shortest_exact_span() {
        assert_eq!(format_span(259_200), "3d");
        assert_eq!(format_span(5_400), "90m");
        assert_eq!(format_span(90_061), "90061s");
        assert_eq!(format_span(0), "0s");
    }

    #[test]
    fn format_round_trips_through_parse() {
        for text in ["45s", "90m", "12h", "3d", "2w"] {
            let seconds = parse_span(text).unwrap();
            assert_eq!(format_span(seconds), text);
        }
    }
}
