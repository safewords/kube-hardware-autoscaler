//! `spec.activeHours`: the hours a machine may be run by the pools.

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::Tz;

use crate::crd::{ActiveHours, ActiveWindow, Day};

/// Where a machine stands against its active hours at one instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Standing {
    /// Inside a window (or no `activeHours`).
    Active,
    /// Outside every window. The text says which hours and when the next
    /// window opens, for the decision log; it only changes when they do.
    Outside(String),
    /// `activeHours` could not be read (an unknown time zone, a bad time).
    /// Treated as outside, so a mistake never wakes a machine.
    Invalid(String),
}

impl Standing {
    /// The note for the pools' decisions, when not active.
    pub fn note(&self) -> Option<String> {
        match self {
            Standing::Active => None,
            Standing::Outside(s) => Some(format!("outside active hours ({s})")),
            Standing::Invalid(e) => Some(format!("activeHours invalid ({e}); treated as outside")),
        }
    }
}

fn weekday(d: Day) -> Weekday {
    match d {
        Day::Mon => Weekday::Mon,
        Day::Tue => Weekday::Tue,
        Day::Wed => Weekday::Wed,
        Day::Thu => Weekday::Thu,
        Day::Fri => Weekday::Fri,
        Day::Sat => Weekday::Sat,
        Day::Sun => Weekday::Sun,
    }
}

/// `HH:MM` (or `24:00`) as minutes since midnight.
fn minutes(s: &str) -> Result<u32, String> {
    let bad = || format!("time {s:?} is not HH:MM");
    let (h, m) = s.split_once(':').ok_or_else(bad)?;
    let (h, m): (u32, u32) = (h.parse().map_err(|_| bad())?, m.parse().map_err(|_| bad())?);
    if h == 24 && m == 0 {
        return Ok(24 * 60);
    }
    if h > 23 || m > 59 {
        return Err(bad());
    }
    Ok(h * 60 + m)
}

struct Window {
    days: Vec<Weekday>,
    start: u32,
    end: u32,
}

impl Window {
    fn parse(w: &ActiveWindow) -> Result<Self, String> {
        let start = minutes(&w.start)?;
        if start == 24 * 60 {
            return Err("start may not be 24:00".into());
        }
        Ok(Window {
            days: w.days.iter().copied().map(weekday).collect(),
            start,
            end: minutes(&w.end)?,
        })
    }
    fn on(&self, d: Weekday) -> bool {
        self.days.is_empty() || self.days.contains(&d)
    }
    /// Whether the window covers local time `t` (minutes) on day `d`.
    fn covers(&self, d: Weekday, t: u32) -> bool {
        if self.start < self.end {
            self.on(d) && self.start <= t && t < self.end
        } else {
            // Past midnight: the evening part on its own day, the morning
            // part on the day after.
            (self.on(d) && t >= self.start) || (self.on(d.pred()) && t < self.end)
        }
    }
}

fn describe(h: &ActiveHours, tz: &str) -> String {
    let windows = h
        .windows
        .iter()
        .map(|w| {
            let days = if w.days.is_empty() {
                String::new()
            } else {
                let d: Vec<String> = w.days.iter().map(|d| format!("{d:?}")).collect();
                format!("{} ", d.join(","))
            };
            format!("{days}{}-{}", w.start, w.end)
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{windows} {tz}")
}

/// Local midnight-relative instant, resolving DST gaps forward and folds to
/// the earlier reading.
fn local(tz: Tz, date: NaiveDate, mins: u32) -> Option<DateTime<Tz>> {
    let base = date.and_time(NaiveTime::MIN) + Duration::minutes(mins as i64);
    tz.from_local_datetime(&base)
        .earliest()
        .or_else(|| tz.from_local_datetime(&(base + Duration::hours(1))).earliest())
}

/// Where a machine with `hours` stands at `now`.
pub fn standing(hours: Option<&ActiveHours>, now: DateTime<Utc>) -> Standing {
    let Some(h) = hours else {
        return Standing::Active;
    };
    let tz_name = h.time_zone.as_deref().unwrap_or("UTC");
    let tz: Tz = match tz_name.parse() {
        Ok(tz) => tz,
        Err(_) => return Standing::Invalid(format!("unknown time zone {tz_name:?}")),
    };
    let windows: Vec<Window> = match h.windows.iter().map(Window::parse).collect() {
        Ok(w) => w,
        Err(e) => return Standing::Invalid(e),
    };
    if windows.is_empty() {
        return Standing::Invalid("no windows".into());
    }
    let here = now.with_timezone(&tz);
    let t = here.hour() * 60 + here.minute();
    if windows.iter().any(|w| w.covers(here.weekday(), t)) {
        return Standing::Active;
    }
    // The next window to open, within the coming week.
    let next = (0..=7)
        .flat_map(|d| {
            let date = here.date_naive() + Duration::days(d);
            windows
                .iter()
                .filter(move |w| w.on(date.weekday()))
                .filter_map(move |w| local(tz, date, w.start))
        })
        .filter(|t| *t > here)
        .min();
    let what = describe(h, tz_name);
    Standing::Outside(match next {
        Some(n) => format!("{what}; next opens {}", n.format("%a %H:%M %Z")),
        None => what,
    })
}

/// When a machine with `hours` stops being active, if it is active at `now`:
/// the first minute no window covers, following windows that run into each
/// other (and across midnight). `None` when it is not active at `now`, when
/// it has no `activeHours`, or when it stays active for the coming week.
pub fn active_until(hours: Option<&ActiveHours>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let h = hours?;
    let tz: Tz = h.time_zone.as_deref().unwrap_or("UTC").parse().ok()?;
    let windows: Vec<Window> = h.windows.iter().map(Window::parse).collect::<Result<_, _>>().ok()?;
    let covered = |at: DateTime<Utc>| {
        let here = at.with_timezone(&tz);
        let t = here.hour() * 60 + here.minute();
        windows.iter().any(|w| w.covers(here.weekday(), t))
    };
    if !covered(now) {
        return None;
    }
    // Minute by minute, from the next minute boundary: windows are whole
    // minutes, and a week is ten thousand steps.
    let first = now - Duration::seconds(now.second() as i64) - Duration::nanoseconds(now.nanosecond() as i64)
        + Duration::minutes(1);
    (0..7 * 24 * 60)
        .map(|m| first + Duration::minutes(m))
        .find(|t| !covered(*t))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hours(tz: &str, windows: &[(&[Day], &str, &str)]) -> ActiveHours {
        ActiveHours {
            time_zone: Some(tz.into()),
            windows: windows
                .iter()
                .map(|(d, s, e)| ActiveWindow {
                    days: d.to_vec(),
                    start: s.to_string(),
                    end: e.to_string(),
                })
                .collect(),
        }
    }

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn none_is_always_active() {
        assert_eq!(standing(None, Utc::now()), Standing::Active);
    }

    #[test]
    fn daytime_in_los_angeles() {
        let h = hours("America/Los_Angeles", &[(&[], "08:00", "22:00")]);
        // 2026-11-17 is a Tuesday; PST is UTC-8.
        assert_eq!(standing(Some(&h), at("2026-11-17T16:00:00Z")), Standing::Active); // 08:00
        assert_eq!(standing(Some(&h), at("2026-11-18T05:59:00Z")), Standing::Active); // 21:59
        assert_eq!(
            standing(Some(&h), at("2026-11-18T06:00:00Z")), // 22:00
            Standing::Outside("08:00-22:00 America/Los_Angeles; next opens Wed 08:00 PST".into())
        );
        assert_eq!(
            standing(Some(&h), at("2026-11-17T15:59:00Z")), // 07:59
            Standing::Outside("08:00-22:00 America/Los_Angeles; next opens Tue 08:00 PST".into())
        );
    }

    #[test]
    fn follows_daylight_saving() {
        let h = hours("America/Los_Angeles", &[(&[], "08:00", "22:00")]);
        // July: PDT, UTC-7. 15:30Z is 08:30 local, 07:30 in winter time.
        assert_eq!(standing(Some(&h), at("2026-07-14T15:30:00Z")), Standing::Active);
        assert!(matches!(
            standing(Some(&h), at("2026-12-15T15:30:00Z")),
            Standing::Outside(_)
        ));
    }

    #[test]
    fn weekdays_only() {
        let h = hours(
            "UTC",
            &[(&[Day::Mon, Day::Tue, Day::Wed, Day::Thu, Day::Fri], "09:00", "17:00")],
        );
        assert_eq!(standing(Some(&h), at("2026-11-20T12:00:00Z")), Standing::Active); // Fri
        assert_eq!(
            standing(Some(&h), at("2026-11-21T12:00:00Z")), // Sat
            Standing::Outside("Mon,Tue,Wed,Thu,Fri 09:00-17:00 UTC; next opens Mon 09:00 UTC".into())
        );
    }

    #[test]
    fn past_midnight_belongs_to_the_starting_day() {
        let h = hours("UTC", &[(&[Day::Fri], "22:00", "06:00")]);
        assert_eq!(standing(Some(&h), at("2026-11-20T23:00:00Z")), Standing::Active); // Fri night
        assert_eq!(standing(Some(&h), at("2026-11-21T05:00:00Z")), Standing::Active); // Sat morning
        assert!(matches!(
            standing(Some(&h), at("2026-11-21T07:00:00Z")),
            Standing::Outside(_)
        ));
        assert!(matches!(
            standing(Some(&h), at("2026-11-20T05:00:00Z")),
            Standing::Outside(_)
        )); // Fri morning
    }

    #[test]
    fn whole_day_and_several_windows() {
        let h = hours("UTC", &[(&[Day::Sat], "00:00", "24:00"), (&[], "12:00", "13:00")]);
        assert_eq!(standing(Some(&h), at("2026-11-21T03:00:00Z")), Standing::Active); // Sat
        assert_eq!(standing(Some(&h), at("2026-11-18T12:30:00Z")), Standing::Active); // Wed lunch
        assert!(matches!(
            standing(Some(&h), at("2026-11-18T14:00:00Z")),
            Standing::Outside(_)
        ));
    }

    #[test]
    fn active_until_finds_the_end_of_the_window() {
        let day = hours("America/Los_Angeles", &[(&[], "08:00", "20:00")]);
        // 19:55 PST is 03:55 UTC the next day.
        assert_eq!(
            active_until(Some(&day), at("2026-11-18T03:55:30Z")),
            Some(at("2026-11-18T04:00:00Z"))
        );
        assert_eq!(
            active_until(Some(&day), at("2026-11-17T17:00:00Z")),
            Some(at("2026-11-18T04:00:00Z"))
        );
        assert_eq!(
            active_until(Some(&day), at("2026-11-18T05:00:00Z")),
            None,
            "outside: not active"
        );
        assert_eq!(
            active_until(None, at("2026-11-18T05:00:00Z")),
            None,
            "no hours: never closes"
        );
        // Windows that meet run on; one that covers the whole week never closes.
        let chained = hours("UTC", &[(&[], "08:00", "12:00"), (&[], "12:00", "14:00")]);
        assert_eq!(
            active_until(Some(&chained), at("2026-11-17T09:00:00Z")),
            Some(at("2026-11-17T14:00:00Z"))
        );
        let always = hours("UTC", &[(&[], "00:00", "24:00")]);
        assert_eq!(active_until(Some(&always), at("2026-11-17T09:00:00Z")), None);
        let overnight = hours("UTC", &[(&[], "22:00", "06:00")]);
        assert_eq!(
            active_until(Some(&overnight), at("2026-11-17T23:00:00Z")),
            Some(at("2026-11-18T06:00:00Z"))
        );
    }

    #[test]
    fn mistakes_count_as_outside() {
        let h = hours("Mars/Olympus_Mons", &[(&[], "08:00", "22:00")]);
        assert!(matches!(standing(Some(&h), Utc::now()), Standing::Invalid(_)));
        let h = hours("UTC", &[(&[], "8am", "22:00")]);
        assert!(matches!(standing(Some(&h), Utc::now()), Standing::Invalid(_)));
    }
}
