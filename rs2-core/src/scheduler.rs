//! Host-driven scheduler (G1): periodic invocation of a mount.
//!
//! A mount declares `config.schedule` (`{ "every": "60s" }` or
//! `{ "cron": "0 9 * * *" }`); the runtime fires a synthetic internal request at
//! it on cadence (see [`crate::runtime::Runtime::spawn_scheduler`]). This module
//! is the pure, host-agnostic core: schedule parsing, cron next-occurrence math,
//! the interval occurrence bucket, and the [`ScheduleStore`] coordination
//! adapter (in-memory default; swap for Redis to run HA).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use time::OffsetDateTime;

use crate::error::RsError;
use crate::message::{Message, Source};

/// The synthetic request the scheduler fires at a mount: an internal `POST` to
/// the mount root, tagged so the service can tell a tick from real traffic.
pub fn tick_message(tenant: &str, base_path: &str) -> Message {
    let path = if base_path.is_empty() { "/" } else { base_path };
    let mut msg = Message::request(http::Method::POST, path, tenant);
    // A tick is a trusted system call: the operator configured `schedule` on
    // this mount, and the source cannot be forged from the wire.
    msg.source = Source::System;
    msg.set_header("x-rs2-trigger", "schedule");
    msg
}

/// A parsed mount schedule.
#[derive(Debug, Clone)]
pub enum Schedule {
    /// Fire every fixed period, aligned to epoch buckets.
    Interval(Duration),
    /// Fire at 5-field cron occurrences (UTC).
    Cron(CronSchedule),
}

impl Schedule {
    /// Parse `config.schedule`. Exactly one of `every` / `cron` is required;
    /// both or neither is an error. Used by `Tenant::build` (validate) and the
    /// scheduler (use), so a bad value is a 400 at `PUT /raw`.
    pub fn from_config(v: &Value) -> Result<Self, RsError> {
        let every = v.get("every").and_then(|x| x.as_str());
        let cron = v.get("cron").and_then(|x| x.as_str());
        match (every, cron) {
            (Some(_), Some(_)) => Err(RsError::bad_request(
                "schedule has both 'every' and 'cron' — set exactly one",
            )),
            (Some(e), None) => Ok(Schedule::Interval(parse_every(e)?)),
            (None, Some(c)) => Ok(Schedule::Cron(parse_cron(c)?)),
            (None, None) => Err(RsError::bad_request(
                "schedule requires 'every' (e.g. \"60s\") or 'cron' (e.g. \"0 9 * * *\")",
            )),
        }
    }
}

/// `"500ms" | "30s" | "5m" | "2h"` → [`Duration`]. Rejects 0, empty, unknown
/// units, and overflow.
pub fn parse_every(s: &str) -> Result<Duration, RsError> {
    let bad = || {
        RsError::bad_request(format!(
            "invalid interval '{s}' (use e.g. 500ms, 30s, 5m, 2h)"
        ))
    };
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| bad())?;
    if n == 0 {
        return Err(bad());
    }
    let mult_ms: u64 = match unit {
        "ms" => 1,
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => return Err(bad()),
    };
    n.checked_mul(mult_ms)
        .map(Duration::from_millis)
        .ok_or_else(bad)
}

/// The epoch-aligned occurrence bucket for an interval at `now` — the same
/// value on every node, so claims race for one key.
pub fn interval_bucket_ms(now: OffsetDateTime, every: Duration) -> i64 {
    let ms = (now.unix_timestamp_nanos() / 1_000_000) as i64;
    let period = (every.as_millis().max(1)) as i64;
    (ms / period) * period
}

// ---- cron ---------------------------------------------------------------

/// One parsed cron field: `*` or an explicit set of allowed values.
#[derive(Debug, Clone)]
enum CronField {
    Any,
    Set(Vec<u8>),
}

impl CronField {
    fn matches(&self, v: u8) -> bool {
        match self {
            CronField::Any => true,
            CronField::Set(s) => s.contains(&v),
        }
    }

    /// Parse one field over `[min, max]`: `*`, `a`, `a-b`, `*/n`, `a-b/n`, and
    /// comma lists thereof.
    fn parse(spec: &str, min: u8, max: u8) -> Result<CronField, RsError> {
        let bad = || RsError::bad_request(format!("invalid cron field '{spec}'"));
        if spec == "*" {
            return Ok(CronField::Any);
        }
        let mut set: Vec<u8> = Vec::new();
        for part in spec.split(',') {
            let (base, step) = match part.split_once('/') {
                Some((b, s)) => (b, s.parse::<u8>().map_err(|_| bad())?),
                None => (part, 1),
            };
            if step == 0 {
                return Err(bad());
            }
            let (lo, hi) = if base == "*" {
                (min, max)
            } else if let Some((a, b)) = base.split_once('-') {
                (
                    a.parse::<u8>().map_err(|_| bad())?,
                    b.parse::<u8>().map_err(|_| bad())?,
                )
            } else {
                let v: u8 = base.parse().map_err(|_| bad())?;
                (v, v)
            };
            if lo < min || hi > max || lo > hi {
                return Err(bad());
            }
            let mut x = lo;
            while x <= hi {
                set.push(x);
                x = x.saturating_add(step);
            }
        }
        set.sort_unstable();
        set.dedup();
        Ok(CronField::Set(set))
    }

    /// Day-of-week: 0-7 with 7 normalized to 0 (both Sunday).
    fn parse_dow(spec: &str) -> Result<CronField, RsError> {
        match CronField::parse(spec, 0, 7)? {
            CronField::Any => Ok(CronField::Any),
            CronField::Set(mut v) => {
                for x in v.iter_mut() {
                    if *x == 7 {
                        *x = 0;
                    }
                }
                v.sort_unstable();
                v.dedup();
                Ok(CronField::Set(v))
            }
        }
    }
}

/// A parsed 5-field cron expression (minute hour day-of-month month
/// day-of-week), evaluated in UTC.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    min: CronField,
    hour: CronField,
    dom: CronField,
    month: CronField,
    dow: CronField,
}

/// `"min hour day-of-month month day-of-week"` (5 fields, UTC).
pub fn parse_cron(s: &str) -> Result<CronSchedule, RsError> {
    let f: Vec<&str> = s.split_whitespace().collect();
    if f.len() != 5 {
        return Err(RsError::bad_request(format!(
            "cron '{s}' must have 5 fields: minute hour day-of-month month day-of-week"
        )));
    }
    Ok(CronSchedule {
        min: CronField::parse(f[0], 0, 59)?,
        hour: CronField::parse(f[1], 0, 23)?,
        dom: CronField::parse(f[2], 1, 31)?,
        month: CronField::parse(f[3], 1, 12)?,
        dow: CronField::parse_dow(f[4])?,
    })
}

impl CronSchedule {
    fn matches(&self, dt: OffsetDateTime) -> bool {
        self.min.matches(dt.minute())
            && self.hour.matches(dt.hour())
            && self.month.matches(u8::from(dt.month()))
            && self.day_matches(dt)
    }

    /// Standard cron day semantics: when both day-of-month and day-of-week are
    /// restricted, a day matches if it matches **either**; when one is `*`, only
    /// the other constrains.
    fn day_matches(&self, dt: OffsetDateTime) -> bool {
        let dom_v = dt.day();
        let dow_v = dt.weekday().number_days_from_sunday();
        match (&self.dom, &self.dow) {
            (CronField::Any, CronField::Any) => true,
            (dom, CronField::Any) => dom.matches(dom_v),
            (CronField::Any, dow) => dow.matches(dow_v),
            (dom, dow) => dom.matches(dom_v) || dow.matches(dow_v),
        }
    }

    /// First UTC minute strictly after `after` that matches, by minute-stepping
    /// with a 366-day bound. `None` if unreachable within the bound. Computed
    /// only when (re)arming a schedule, so the linear scan is cheap.
    pub fn next_occurrence_after(&self, after: OffsetDateTime) -> Option<OffsetDateTime> {
        let mut t =
            after.replace_second(0).ok()?.replace_nanosecond(0).ok()? + time::Duration::minutes(1);
        let bound = after + time::Duration::days(366);
        while t <= bound {
            if self.matches(t) {
                return Some(t);
            }
            t += time::Duration::minutes(1);
        }
        None
    }
}

// ---- coordination adapter ----------------------------------------------

/// Cluster coordination for fire-once scheduling. The default in-memory adapter
/// suits single-node deployments and tests; a shared adapter (Redis: `SET
/// key:occurrence NX PX ttl`) makes a multi-node cluster fire each occurrence
/// exactly once — the only state the scheduler needs to share.
#[async_trait]
pub trait ScheduleStore: Send + Sync {
    /// Atomically claim the right to fire `key`'s occurrence at `occurrence_ms`.
    /// `true` iff this caller won (→ fire); `false` iff already claimed (another
    /// node, or a duplicate). `ttl` bounds how long the claim is remembered
    /// (occurrence claims self-expire so the keyspace stays bounded).
    async fn claim(&self, key: &str, occurrence_ms: i64, ttl: Duration) -> Result<bool, RsError>;
}

/// In-memory [`ScheduleStore`] — single-node correct; the default.
#[derive(Default)]
pub struct MemScheduleStore {
    claimed: Mutex<HashMap<String, std::time::Instant>>,
}

impl MemScheduleStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ScheduleStore for MemScheduleStore {
    async fn claim(&self, key: &str, occurrence_ms: i64, ttl: Duration) -> Result<bool, RsError> {
        let composite = format!("{key}:{occurrence_ms}");
        let now = std::time::Instant::now();
        let mut claimed = self.claimed.lock().unwrap();
        claimed.retain(|_, exp| *exp > now);
        if let std::collections::hash_map::Entry::Vacant(e) = claimed.entry(composite) {
            e.insert(now + ttl);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(y: i32, mo: u8, d: u8, h: u8, mi: u8) -> OffsetDateTime {
        let date =
            time::Date::from_calendar_date(y, time::Month::try_from(mo).unwrap(), d).unwrap();
        let time = time::Time::from_hms(h, mi, 0).unwrap();
        time::PrimitiveDateTime::new(date, time).assume_utc()
    }

    #[test]
    fn parse_every_valid_and_invalid() {
        assert_eq!(parse_every("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_every("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_every("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_every("2h").unwrap(), Duration::from_secs(7200));
        for bad in ["", "0s", "10x", "s", "60", "-5s", " "] {
            assert!(parse_every(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn parse_cron_valid_and_invalid() {
        for ok in [
            "* * * * *",
            "0 9 * * *",
            "0,30 * * * *",
            "0-15 * * * *",
            "*/5 * * * *",
            "0-30/10 * * * *",
            "0 0 * * 7",
        ] {
            assert!(parse_cron(ok).is_ok(), "should accept {ok:?}");
        }
        for bad in [
            "* * * *",
            "60 * * * *",
            "* 24 * * *",
            "0 0 0 * *",
            "0 0 * 13 *",
            "a * * * *",
            "*/0 * * * *",
        ] {
            assert!(parse_cron(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn cron_next_occurrence_daily() {
        let c = parse_cron("0 9 * * *").unwrap();
        // 08:59 -> 09:00 today.
        assert_eq!(
            c.next_occurrence_after(dt(2026, 6, 16, 8, 59)),
            Some(dt(2026, 6, 16, 9, 0))
        );
        // exactly 09:00 -> tomorrow (strictly after).
        assert_eq!(
            c.next_occurrence_after(dt(2026, 6, 16, 9, 0)),
            Some(dt(2026, 6, 17, 9, 0))
        );
    }

    #[test]
    fn cron_year_rollover() {
        let c = parse_cron("0 0 1 1 *").unwrap(); // Jan 1 00:00
        assert_eq!(
            c.next_occurrence_after(dt(2026, 12, 31, 23, 59)),
            Some(dt(2027, 1, 1, 0, 0))
        );
    }

    #[test]
    fn cron_dom_dow_or_semantics() {
        // Both restricted -> OR. 2026-11-13 is a Friday.
        let both = parse_cron("0 0 13 * 5").unwrap();
        assert!(both.matches(dt(2026, 11, 13, 0, 0)), "13th matches");
        assert!(
            both.matches(dt(2026, 11, 6, 0, 0)),
            "a Friday (the 6th) matches via DOW"
        );
        // Only DOM restricted.
        let dom_only = parse_cron("0 0 13 * *").unwrap();
        assert!(dom_only.matches(dt(2026, 11, 13, 0, 0)));
        assert!(
            !dom_only.matches(dt(2026, 11, 6, 0, 0)),
            "non-13th day must not match"
        );
        // Only DOW restricted (Fridays).
        let dow_only = parse_cron("0 0 * * 5").unwrap();
        assert!(dow_only.matches(dt(2026, 11, 6, 0, 0)));
        assert!(
            !dow_only.matches(dt(2026, 11, 5, 0, 0)),
            "Thursday must not match"
        );
    }

    #[test]
    fn interval_bucket_is_epoch_aligned() {
        let every = Duration::from_secs(60);
        let a = interval_bucket_ms(
            OffsetDateTime::from_unix_timestamp(1_000_000_030).unwrap(),
            every,
        );
        let b = interval_bucket_ms(
            OffsetDateTime::from_unix_timestamp(1_000_000_059).unwrap(),
            every,
        );
        let c = interval_bucket_ms(
            OffsetDateTime::from_unix_timestamp(1_000_000_081).unwrap(),
            every,
        );
        assert_eq!(a, b, "same minute bucket");
        assert_ne!(b, c, "next minute is a new bucket");
        assert_eq!(a % 60_000, 0, "bucket aligned to the period");
    }

    #[test]
    fn tick_message_is_an_internal_post_with_trigger_header() {
        let msg = tick_message("t1", "/job");
        assert_eq!(msg.method, http::Method::POST);
        assert_eq!(msg.source, Source::System);
        assert!(msg.principal.is_none());
        assert_eq!(msg.header("x-rs2-trigger"), Some("schedule"));
        assert_eq!(msg.url.path, "/job");
        assert_eq!(
            tick_message("t1", "").url.path,
            "/",
            "root mount fires at /"
        );
    }

    #[tokio::test]
    async fn mem_schedule_store_claims_once_per_occurrence() {
        let store = MemScheduleStore::new();
        let ttl = Duration::from_secs(60);
        assert!(store.claim("t|/m", 1000, ttl).await.unwrap(), "first wins");
        assert!(
            !store.claim("t|/m", 1000, ttl).await.unwrap(),
            "repeat loses"
        );
        assert!(
            store.claim("t|/m", 2000, ttl).await.unwrap(),
            "new occurrence wins"
        );
        assert!(
            store.claim("t|/other", 1000, ttl).await.unwrap(),
            "different key wins"
        );
    }
}
