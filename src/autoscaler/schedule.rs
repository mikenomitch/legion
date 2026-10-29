//! Schedules: cron windows that raise the floor or lower the ceiling
//! (plan §6.4, "Schedules are bound overrides, not a strategy").
//!
//! ```nickel
//! schedule = [
//!   { cron = "0 8 * * 1-5", for = "10h", min = 10, timezone = "America/New_York" },
//! ]
//! ```
//!
//! # Syntax
//!
//! `cron` is a standard five-field crontab expression, `minute hour
//! day-of-month month day-of-week`, with Sunday as `0` or `7` and names
//! (`mon-fri`) accepted, or one of `@hourly`, `@daily` (`@midnight`),
//! `@weekly`, `@monthly`, `@yearly` (`@annually`). Each field is parsed by
//! the `cron` crate Legion already uses for batch schedules; the
//! day-of-week field is translated first, because that crate numbers days
//! from Sunday = 1. As in crontab, when both day-of-month and day-of-week
//! are restricted, a day matches if *either* does.
//!
//! # Time zones and daylight saving
//!
//! `timezone` is an IANA name (default `UTC`), and windows follow that
//! zone's wall clock: `0 8 * * 1-5` in `America/New_York` opens at 08:00
//! local on both sides of a DST change (13:00 UTC in winter, 12:00 UTC in
//! summer). Two edge cases:
//!
//! - **A start that falls in a spring-forward gap** (02:30 on the night
//!   clocks jump from 02:00 to 03:00) is read with the offset in force just
//!   before the gap, so the window opens at 03:30 and still lasts `for`.
//! - **A start that occurs twice** in a fall-back hour opens once, at the
//!   first occurrence.
//!
//! Windows are evaluated from `now` alone, with no state, so a restarted
//! autoscaler sees exactly the windows the previous one did.

use std::collections::BTreeSet;
use std::str::FromStr;
use std::time::Duration;

use chrono::{
    DateTime, Datelike, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeDelta, TimeZone, Utc,
};
use chrono_tz::Tz;
use cron::TimeUnitSpec;

use super::defaults::EffectiveSchedule;

const DAY_NAMES: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const DAY_MS: i64 = 24 * 3600 * 1000;

/// A parsed schedule expression in a time zone.
#[derive(Debug, Clone)]
pub struct CronWindow {
    expression: String,
    timezone: Tz,
    schedule: cron::Schedule,
    hours: Vec<u32>,
    minutes: Vec<u32>,
    dom_restricted: bool,
    dow_restricted: bool,
}

/// One open window, as unix milliseconds: `[start_ms, end_ms)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// When the window opened.
    pub start_ms: i64,
    /// When it closes (exclusive).
    pub end_ms: i64,
}

/// Parse an IANA time zone name, with an error that says what to write.
pub fn parse_timezone(name: &str) -> Result<Tz, String> {
    Tz::from_str(name.trim()).map_err(|_| {
        format!("unknown time zone '{name}'. Hint: use an IANA name like America/New_York or UTC")
    })
}

/// Translate a crontab day-of-week field (Sunday = 0 or 7) into the `cron`
/// crate's form (day names, which mean the same in both numberings).
fn translate_day_of_week(field: &str) -> Result<String, String> {
    if field == "*" || field == "?" {
        return Ok("*".to_string());
    }
    let day = |text: &str| -> Result<u32, String> {
        if let Ok(number) = text.parse::<u32>() {
            if number <= 7 {
                return Ok(number);
            }
            return Err(format!(
                "day-of-week {number} is out of range (0-7, Sunday = 0 or 7)"
            ));
        }
        let lower = text.to_ascii_lowercase();
        DAY_NAMES
            .iter()
            .position(|name| lower.starts_with(&name.to_ascii_lowercase()) && lower.len() >= 3)
            .map(|index| index as u32)
            .ok_or_else(|| format!("'{text}' is not a day of the week"))
    };
    let mut days = BTreeSet::new();
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((range, step)) => {
                let step: u32 = step
                    .parse()
                    .map_err(|_| format!("day-of-week step '{step}' is not a number"))?;
                if step == 0 {
                    return Err("day-of-week step must be at least 1".to_string());
                }
                (range, step)
            }
            None => (part, 1),
        };
        let (low, high) = if range == "*" {
            (0, 6)
        } else if let Some((from, to)) = range.split_once('-') {
            (day(from)?, day(to)?)
        } else {
            let single = day(range)?;
            if part.contains('/') {
                (single, 6)
            } else {
                (single, single)
            }
        };
        if low > high {
            return Err(format!(
                "day-of-week range '{range}' runs backwards. Hint: list the days, like sat,sun"
            ));
        }
        let mut current = low;
        while current <= high {
            days.insert(current % 7);
            current += step;
        }
    }
    Ok(days
        .iter()
        .map(|index| DAY_NAMES[*index as usize])
        .collect::<Vec<_>>()
        .join(","))
}

impl CronWindow {
    /// Parse a crontab expression (see the module docs) in an IANA time
    /// zone.
    pub fn parse(expression: &str, timezone: &str) -> Result<CronWindow, String> {
        let timezone = parse_timezone(timezone)?;
        let text = expression.trim();
        let hint = "Hint: five fields: minute hour day-of-month month day-of-week, like \
                    \"0 8 * * 1-5\"";
        let (crate_expression, dom_restricted, dow_restricted) = if text.starts_with('@') {
            let shorthand = match text {
                "@annually" | "@yearly" => "@yearly",
                "@monthly" => "@monthly",
                "@weekly" => "@weekly",
                "@daily" | "@midnight" => "@daily",
                "@hourly" => "@hourly",
                other => {
                    return Err(format!(
                        "cron '{other}' is not a known shorthand (@hourly, @daily, @weekly, \
                         @monthly, @yearly). {hint}"
                    ))
                }
            };
            (shorthand.to_string(), false, false)
        } else {
            let fields: Vec<&str> = text.split_whitespace().collect();
            if fields.len() != 5 {
                return Err(format!(
                    "cron '{text}' has {} fields; it needs five. {hint}",
                    fields.len()
                ));
            }
            let day_of_week =
                translate_day_of_week(fields[4]).map_err(|err| format!("cron '{text}': {err}"))?;
            let restricted = |field: &str| !(field.starts_with('*') || field == "?");
            (
                format!(
                    "0 {} {} {} {} {}",
                    fields[0], fields[1], fields[2], fields[3], day_of_week
                ),
                restricted(fields[2]),
                restricted(fields[4]),
            )
        };
        let schedule = cron::Schedule::from_str(&crate_expression)
            .map_err(|err| format!("cron '{text}': {err}. {hint}"))?;
        let hours: Vec<u32> = schedule.hours().iter().collect();
        let minutes: Vec<u32> = schedule.minutes().iter().collect();
        Ok(CronWindow {
            expression: text.to_string(),
            timezone,
            schedule,
            hours,
            minutes,
            dom_restricted,
            dow_restricted,
        })
    }

    /// The expression as written.
    pub fn expression(&self) -> &str {
        &self.expression
    }

    /// The zone the expression is evaluated in.
    pub fn timezone(&self) -> Tz {
        self.timezone
    }

    fn day_matches(&self, date: NaiveDate) -> bool {
        if !self.schedule.months().includes(date.month()) {
            return false;
        }
        let dom = self.schedule.days_of_month().includes(date.day());
        let dow = self
            .schedule
            .days_of_week()
            .includes(date.weekday().number_from_sunday());
        if self.dom_restricted && self.dow_restricted {
            dom || dow
        } else {
            dom && dow
        }
    }

    /// The window of length `length` that contains `now_ms`, if one is
    /// open. Pure: depends only on `now_ms`.
    pub fn open_window(&self, now_ms: i64, length: Duration) -> Option<Window> {
        let now = DateTime::<Utc>::from_timestamp_millis(now_ms)?;
        let length_ms = i64::try_from(length.as_millis()).unwrap_or(i64::MAX / 2);
        let today = now.with_timezone(&self.timezone).date_naive();
        let days_back = length_ms / DAY_MS + 2;
        for offset in 0..=days_back {
            let date = today - TimeDelta::days(offset);
            if !self.day_matches(date) {
                continue;
            }
            for hour in self.hours.iter().rev() {
                for minute in self.minutes.iter().rev() {
                    let Some(naive) = date.and_hms_opt(*hour, *minute, 0) else {
                        continue;
                    };
                    let start_ms = resolve_local(&self.timezone, naive).timestamp_millis();
                    if start_ms <= now_ms && now_ms < start_ms.saturating_add(length_ms) {
                        return Some(Window {
                            start_ms,
                            end_ms: start_ms.saturating_add(length_ms),
                        });
                    }
                }
            }
        }
        None
    }

    /// The first window start strictly after `after_ms`, within about a
    /// year. For "next window opens at" displays.
    pub fn next_start(&self, after_ms: i64) -> Option<i64> {
        let after = DateTime::<Utc>::from_timestamp_millis(after_ms)?;
        let first = after.with_timezone(&self.timezone).date_naive() - TimeDelta::days(1);
        for offset in 0..=400 {
            let date = first + TimeDelta::days(offset);
            if !self.day_matches(date) {
                continue;
            }
            for hour in &self.hours {
                for minute in &self.minutes {
                    let Some(naive) = date.and_hms_opt(*hour, *minute, 0) else {
                        continue;
                    };
                    let start_ms = resolve_local(&self.timezone, naive).timestamp_millis();
                    if start_ms > after_ms {
                        return Some(start_ms);
                    }
                }
            }
        }
        None
    }

    /// Render an instant in this schedule's zone, as `HH:MM ZONE`.
    pub fn local_time(&self, at_ms: i64) -> String {
        DateTime::<Utc>::from_timestamp_millis(at_ms)
            .map(|at| {
                at.with_timezone(&self.timezone)
                    .format("%H:%M %Z")
                    .to_string()
            })
            .unwrap_or_else(|| at_ms.to_string())
    }
}

/// The instant a local wall time names: the first occurrence when it is
/// ambiguous, and the pre-gap offset when it falls in a spring-forward gap.
fn resolve_local(timezone: &Tz, naive: NaiveDateTime) -> DateTime<Utc> {
    match timezone.from_local_datetime(&naive) {
        LocalResult::Single(at) => at.with_timezone(&Utc),
        LocalResult::Ambiguous(earliest, _) => earliest.with_timezone(&Utc),
        LocalResult::None => {
            let before = timezone
                .offset_from_utc_datetime(&(naive - TimeDelta::days(1)))
                .fix();
            let utc = naive - TimeDelta::seconds(i64::from(before.local_minus_utc()));
            Utc.from_utc_datetime(&utc)
        }
    }
}

/// The bound overrides in force at one instant, with the notes that
/// explain them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScheduledBounds {
    /// The highest floor among open windows, and which schedule set it.
    pub min: Option<(u32, String)>,
    /// The lowest ceiling among open windows, and which schedule set it.
    pub max: Option<(u32, String)>,
}

/// Which of a policy's schedules are open at `now_ms`, folded into one floor
/// (the highest `min`) and one ceiling (the lowest `max`).
///
/// A schedule that fails to parse is skipped; [`resolve`](super::defaults::resolve)
/// rejects such a policy, so this only happens to a hand-built one.
pub fn scheduled_bounds(schedules: &[EffectiveSchedule], now_ms: i64) -> ScheduledBounds {
    let mut bounds = ScheduledBounds::default();
    for schedule in schedules {
        let Ok(window) = CronWindow::parse(&schedule.cron, &schedule.timezone) else {
            continue;
        };
        let Some(open) = window.open_window(now_ms, schedule.duration) else {
            continue;
        };
        let label = format!(
            "schedule '{}' until {}",
            schedule.cron,
            window.local_time(open.end_ms)
        );
        if let Some(min) = schedule.min {
            if bounds
                .min
                .as_ref()
                .is_none_or(|(current, _)| min > *current)
            {
                bounds.min = Some((min, label.clone()));
            }
        }
        if let Some(max) = schedule.max {
            if bounds
                .max
                .as_ref()
                .is_none_or(|(current, _)| max < *current)
            {
                bounds.max = Some((max, label));
            }
        }
    }
    bounds
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
            .unwrap()
            .timestamp_millis()
    }

    fn hours(n: u64) -> Duration {
        Duration::from_secs(n * 3600)
    }

    #[test]
    fn day_of_week_translation_table_test() {
        let cases = [
            ("*", Ok("*")),
            ("1-5", Ok("Mon,Tue,Wed,Thu,Fri")),
            ("0", Ok("Sun")),
            ("7", Ok("Sun")),
            ("*/2", Ok("Sun,Tue,Thu,Sat")),
            ("1-5/2", Ok("Mon,Wed,Fri")),
            ("mon-fri", Ok("Mon,Tue,Wed,Thu,Fri")),
            ("5-7", Ok("Sun,Fri,Sat")),
            ("SAT,sun", Ok("Sun,Sat")),
            ("sat-sun", Err("runs backwards")),
            ("8", Err("out of range")),
            ("1-5/0", Err("at least 1")),
            ("funday", Err("not a day of the week")),
        ];
        for (field, expected) in cases {
            let got = translate_day_of_week(field);
            match expected {
                Ok(text) => assert_eq!(got.as_deref(), Ok(text), "{field}"),
                Err(text) => {
                    let err = got.unwrap_err();
                    assert!(err.contains(text), "{field}: {err}");
                }
            }
        }
    }

    #[test]
    fn parse_errors_test() {
        let cases = [
            ("0 8 * *", "UTC", "has 4 fields"),
            ("0 8 * * * *", "UTC", "has 6 fields"),
            ("@sometimes", "UTC", "not a known shorthand"),
            ("61 8 * * *", "UTC", "cron '61 8 * * *'"),
            ("0 8 * * *", "Atlantis/Central", "unknown time zone"),
        ];
        for (expression, timezone, expected) in cases {
            let err = CronWindow::parse(expression, timezone).unwrap_err();
            assert!(err.contains(expected), "{expression}: {err}");
        }
    }

    #[test]
    fn weekday_window_follows_new_york_wall_clock_across_dst_test() {
        let window = CronWindow::parse("0 8 * * 1-5", "America/New_York").unwrap();
        let length = hours(10);
        // (instant, open?) — 2026-03-08 is the spring-forward Sunday.
        let cases = [
            // Friday in EST: 08:00 local = 13:00 UTC.
            (utc(2026, 3, 6, 12, 59), false),
            (utc(2026, 3, 6, 13, 0), true),
            (utc(2026, 3, 6, 22, 59), true),
            (utc(2026, 3, 6, 23, 0), false),
            // Saturday and Sunday: closed (crontab 1-5 is Monday-Friday).
            (utc(2026, 3, 7, 14, 0), false),
            (utc(2026, 3, 8, 14, 0), false),
            // Monday in EDT: 08:00 local = 12:00 UTC.
            (utc(2026, 3, 9, 11, 59), false),
            (utc(2026, 3, 9, 12, 0), true),
            (utc(2026, 3, 9, 21, 59), true),
            (utc(2026, 3, 9, 22, 0), false),
        ];
        for (at, open) in cases {
            assert_eq!(
                window.open_window(at, length).is_some(),
                open,
                "{}",
                DateTime::<Utc>::from_timestamp_millis(at).unwrap()
            );
        }
        let open = window.open_window(utc(2026, 3, 9, 15, 0), length).unwrap();
        assert_eq!(open.start_ms, utc(2026, 3, 9, 12, 0));
        assert_eq!(window.local_time(open.end_ms), "18:00 EDT");
        assert_eq!(
            window.next_start(utc(2026, 3, 6, 23, 0)),
            Some(utc(2026, 3, 9, 12, 0))
        );
    }

    #[test]
    fn spring_forward_gap_and_fall_back_repeat_test() {
        // 02:30 does not exist on 2026-03-08 in New York: the window opens
        // at 02:30 EST = 07:30 UTC (03:30 EDT) and lasts its full hour.
        let gap = CronWindow::parse("30 2 * * *", "America/New_York").unwrap();
        let length = hours(1);
        assert!(gap.open_window(utc(2026, 3, 8, 7, 15), length).is_none());
        let open = gap.open_window(utc(2026, 3, 8, 7, 45), length).unwrap();
        assert_eq!(open.start_ms, utc(2026, 3, 8, 7, 30));
        assert_eq!(open.end_ms, utc(2026, 3, 8, 8, 30));
        // The next day, 02:30 EDT is 06:30 UTC.
        assert!(gap.open_window(utc(2026, 3, 9, 6, 45), length).is_some());

        // 01:30 happens twice on 2026-11-01: the window opens once, at the
        // first (EDT, 05:30 UTC); the repeat (EST, 06:30 UTC) is not a
        // second window.
        let repeat = CronWindow::parse("30 1 * * *", "America/New_York").unwrap();
        let length = Duration::from_secs(30 * 60);
        assert!(repeat
            .open_window(utc(2026, 11, 1, 5, 45), length)
            .is_some());
        assert!(repeat
            .open_window(utc(2026, 11, 1, 6, 45), length)
            .is_none());
    }

    #[test]
    fn windows_cross_midnight_and_or_day_rule_and_shorthand_test() {
        let night = CronWindow::parse("0 22 * * *", "UTC").unwrap();
        assert!(night.open_window(utc(2026, 3, 3, 1, 0), hours(4)).is_some());
        assert!(night.open_window(utc(2026, 3, 3, 2, 0), hours(4)).is_none());

        // Day-of-month and day-of-week both restricted: either matches.
        let either = CronWindow::parse("0 9 1 * 1", "UTC").unwrap();
        assert!(either
            .open_window(utc(2026, 3, 1, 9, 30), hours(1))
            .is_some()); // Sun 1st
        assert!(either
            .open_window(utc(2026, 3, 2, 9, 30), hours(1))
            .is_some()); // Monday
        assert!(either
            .open_window(utc(2026, 3, 3, 9, 30), hours(1))
            .is_none()); // Tuesday

        let daily = CronWindow::parse("@daily", "UTC").unwrap();
        assert!(daily
            .open_window(utc(2026, 3, 3, 0, 30), hours(1))
            .is_some());
        assert!(daily
            .open_window(utc(2026, 3, 3, 1, 30), hours(1))
            .is_none());
    }

    #[test]
    fn scheduled_bounds_fold_highest_floor_and_lowest_ceiling_test() {
        let schedule = |cron: &str, min: Option<u32>, max: Option<u32>| EffectiveSchedule {
            cron: cron.to_string(),
            duration: hours(10),
            min,
            max,
            timezone: "UTC".to_string(),
        };
        let schedules = vec![
            schedule("0 8 * * *", Some(10), None),
            schedule("0 9 * * *", Some(12), Some(30)),
            schedule("0 7 * * *", None, Some(20)),
            schedule("0 20 * * *", Some(50), None),
        ];
        let bounds = scheduled_bounds(&schedules, utc(2026, 3, 3, 10, 0));
        assert_eq!(bounds.min.as_ref().map(|(n, _)| *n), Some(12));
        assert_eq!(bounds.max.as_ref().map(|(n, _)| *n), Some(20));
        assert!(
            bounds
                .min
                .as_ref()
                .unwrap()
                .1
                .contains("schedule '0 9 * * *' until 19:00 UTC"),
            "{bounds:?}"
        );
        let closed = scheduled_bounds(&schedules, utc(2026, 3, 3, 19, 30));
        assert_eq!(closed, ScheduledBounds::default());
    }
}
