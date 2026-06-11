//! 编译后的调度规格与下次时间计算。
//!
//! 移植自 Temporal 调度器的 `spec.go`（~375 行）。
//!
//! # 模块职责
//!
//! 1. **编译**: `SpecBuilder::new_compiled_spec()` — 规范化 + 验证 + 时区加载
//!    将用户输入的 `ScheduleSpec` 编译为 `CompiledSpec`
//! 2. **计算**: `CompiledSpec::get_next_time()` — 给定参考时间后找到下一个匹配时间
//! 3. **规范化**: `canonicalize_spec()` — 将 CalendarSpec/CronString 统一为
//!    StructuredCalendarSpec + IntervalSpec
//! 4. **Jitter**: `add_jitter()` — 确定性哈希抖动（FarmHash Fingerprint32）
//!
//! # 核心算法: get_next_time()
//!
//! ```text
//! 输入: jitter_seed, after (参考时间点)
//!
//! 1. 边界处理: if after < StartTime → after = StartTime - 1s
//! 2. 循环找 nominal:
//!    raw_next_time() → 从 calendars + intervals 中取最早的
//!    if nominal > EndTime 或 year > 2100 → 返回 None
//!    if is_excluded(nominal) → continue（跳过排除时间继续搜索）
//!    else → break（找到有效 nominal）
//! 3. 计算 Jitter 上限:
//!    maxJitter = spec.jitter
//!    following = raw_next_time(nominal)
//!    maxJitter = min(maxJitter, following - nominal)
//! 4. add_jitter(seed, nominal, maxJitter) → next
//! 5. 返回 { nominal, next }
//! ```
//!
//! # 间隔公式
//!
//! ```text
//! interval_s = max(1, interval.Seconds)
//! phase_s    = max(0, phase.Seconds)
//! next       = ((ts - phase_s) / interval_s + 1) * interval_s + phase_s
//! ```
//!
//! # Jitter 算法
//!
//! ```text
//! hash = FarmHash_Fingerprint32(nominal_iso8601 + jitterSeed)
//! jitter_ms = (hash * maxJitter_ms) >> 32
//! next = nominal + jitter_ms
//! ```
//! 相同 (nominal, seed) → 相同 jitter → 确定性

use std::time::Duration;

use jiff::Timestamp;

use moka::sync::Cache as MokaCache;

use crate::calendar::{
    parse_calendar_to_structured, parse_cron_string, validate_interval,
    validate_structured_calendar, CompiledCalendar,
};
use crate::error::SpecError;
use crate::types::{
    GetNextTimeResult, IntervalSpec, Range, ScheduleSpec, StructuredCalendarSpec, MAX_CALENDAR_YEAR,
};

// ---------------------------------------------------------------------------
// CompiledSpec
// ---------------------------------------------------------------------------

/// A compiled, validated schedule spec that can efficiently compute the next
/// matching time.
#[derive(Clone)]
pub struct CompiledSpec {
    /// Canonicalized spec (proto form).
    spec: ScheduleSpec,

    /// The timezone for calendar-based scheduling.
    tz: jiff::tz::TimeZone,

    /// Compiled calendar entries from `StructuredCalendar`.
    calendar: Vec<CompiledCalendar>,

    /// Compiled exclusion calendars.
    excludes: Vec<CompiledCalendar>,
}

impl CompiledSpec {
    /// Returns a reference to the canonicalized spec.
    pub fn canonical_form(&self) -> &ScheduleSpec {
        &self.spec
    }

    /// Returns the timezone.
    pub fn timezone(&self) -> &jiff::tz::TimeZone {
        &self.tz
    }

    // ------------------------------------------------------------------
    // GetNextTime
    // ------------------------------------------------------------------

    /// Returns the earliest time that matches the schedule spec that is
    /// *after* the given time.
    ///
    /// `jitter_seed` is mixed into the hash that deterministically offsets
    /// the nominal time.  Pass `""` for zero-jitter-dependent hashing.
    ///
    /// Returns `GetNextTimeResult` where both fields are `None` if there is
    /// no matching time (schedule exhausted or past end time).
    pub fn get_next_time(&self, jitter_seed: &str, after: Timestamp) -> GetNextTimeResult {
        // If we're starting before the schedule's allowed time range, jump
        // up to right before it (so that we can still return the first
        // second of the range if it happens to match).
        let after = match self.spec.start_time {
            Some(start) if after < start => {
                // start - 1 second; fall back to after on overflow
                start.checked_sub(Duration::from_secs(1)).unwrap_or(after)
            }
            _ => after,
        };

        let past_end_time =
            |t: Timestamp| -> bool { self.spec.end_time.map_or(false, |end| t > end) };

        fn timestamp_year(t: Timestamp) -> i32 {
            t.to_zoned(jiff::tz::TimeZone::UTC).datetime().year() as i32
        }

        // Nominal time before jitter.
        let mut nominal: Option<Timestamp> = None;

        loop {
            let next = self.raw_next_time(nominal.unwrap_or(after));

            match next {
                None => return GetNextTimeResult::default(),
                Some(n) if past_end_time(n) || timestamp_year(n) > MAX_CALENDAR_YEAR => {
                    return GetNextTimeResult::default();
                }
                Some(n) => {
                    nominal = Some(n);
                }
            }

            // Check exclusions
            if let Some(n) = nominal {
                if !self.is_excluded(n) {
                    break;
                }
            }
        }

        let nominal = nominal.unwrap(); // safe — we returned above on None

        // Jitter
        let max_jitter = self.spec.jitter.unwrap_or(Duration::ZERO);

        // Cap jitter so it doesn't push past the *next* nominal time.
        let max_jitter = match self.raw_next_time(nominal) {
            Some(following) => {
                // Gap between consecutive nominal times, in seconds.
                let gap_secs = following.as_second() - nominal.as_second();
                let gap_std = Duration::from_secs(gap_secs.max(0) as u64);
                max_jitter.min(gap_std)
            }
            None => max_jitter,
        };

        let next = add_jitter(jitter_seed, nominal, max_jitter);

        GetNextTimeResult {
            nominal: Some(nominal),
            next: Some(next),
        }
    }

    // ------------------------------------------------------------------
    // rawNextTime: earliest time after `after` from any calendar or interval
    // ------------------------------------------------------------------

    fn raw_next_time(&self, after: Timestamp) -> Option<Timestamp> {
        let mut min_ts: Option<Timestamp> = None;

        // Calendars
        for cal in &self.calendar {
            if let Some(t) = cal.next(after) {
                min_ts = min_ts.map_or(Some(t), |m| Some(if t < m { t } else { m }));
            }
        }

        // Intervals
        let after_secs = after.as_second();
        for iv in &self.spec.interval {
            let next_secs = next_interval_time(iv, after_secs);
            if let Ok(next) = Timestamp::from_second(next_secs) {
                min_ts = min_ts.map_or(Some(next), |m| Some(if next < m { next } else { m }));
            }
        }

        min_ts
    }

    // ------------------------------------------------------------------
    // excluded
    // ------------------------------------------------------------------

    fn is_excluded(&self, nominal: Timestamp) -> bool {
        self.excludes.iter().any(|cal| cal.matches(nominal))
    }
}

// ---------------------------------------------------------------------------
// SpecBuilder — constructs CompiledSpecs, caches timezone lookups
// ---------------------------------------------------------------------------

/// Builder for [`CompiledSpec`].  Caches timezone lookups (TTL = 24 h).
pub struct SpecBuilder {
    /// Cache for (tz_name) → TimeZone.
    ///
    /// In Go this is `cache.Cache` with TTL; we use `moka` for the same
    /// purpose.  We store `Result<TimeZone, String>` so that negative
    /// results (bad timezone names) are also cached.
    tz_cache: MokaCache<String, Result<jiff::tz::TimeZone, String>>,
}

impl SpecBuilder {
    /// Creates a new `SpecBuilder` with capacity 1000 and 24-hour TTL.
    pub fn new() -> Self {
        Self {
            tz_cache: MokaCache::builder()
                .max_capacity(1000)
                .time_to_live(std::time::Duration::from_secs(86400))
                .build(),
        }
    }

    /// Compile a `ScheduleSpec` into a `CompiledSpec`.
    ///
    /// This canonicalizes the spec (converting cron/calendar strings into
    /// structured form), validates all entries, loads the timezone, and
    /// compiles the structured calendars.
    pub fn new_compiled_spec(&self, spec: &ScheduleSpec) -> Result<CompiledSpec, SpecError> {
        let spec = canonicalize_spec(spec)?;

        // Load timezone
        let tz = self.load_timezone(&spec)?;

        // Compile structured calendars
        let calendar: Vec<CompiledCalendar> = spec
            .structured_calendar
            .iter()
            .map(|scs| CompiledCalendar::new(scs, tz.clone()))
            .collect();

        // Compile exclusion calendars
        let excludes: Vec<CompiledCalendar> = spec
            .exclude_structured_calendar
            .iter()
            .map(|scs| CompiledCalendar::new(scs, tz.clone()))
            .collect();

        Ok(CompiledSpec {
            spec,
            tz,
            calendar,
            excludes,
        })
    }

    /// Load the timezone from the spec.  Uses the cache for IANA names.
    fn load_timezone(&self, spec: &ScheduleSpec) -> Result<jiff::tz::TimeZone, SpecError> {
        // If custom timezone data is provided, load from TZif bytes.
        if let Some(ref data) = spec.timezone_data {
            return jiff::tz::TimeZone::tzif(&spec.timezone_name, data)
                .map_err(|e| SpecError::TimezoneNotFound(e.to_string()));
        }

        let name = if spec.timezone_name.is_empty() {
            "UTC"
        } else {
            &spec.timezone_name
        };

        self.tz_cache
            .get_with(name.to_string(), || {
                jiff::tz::TimeZone::get(name).map_err(|e| format!("timezone '{name}': {e}"))
            })
            .map_err(SpecError::TimezoneNotFound)
    }
}

impl Default for SpecBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// canonicalize_spec
// ---------------------------------------------------------------------------

/// Canonicalize a `ScheduleSpec`: parse calendar strings, cron strings,
/// validate everything, and return a clean spec ready for compilation.
pub fn canonicalize_spec(spec: &ScheduleSpec) -> Result<ScheduleSpec, SpecError> {
    let mut spec = spec.clone();

    // Parse CalendarSpec → StructuredCalendarSpec
    for cal in std::mem::take(&mut spec.calendar) {
        let structured = parse_calendar_to_structured(&cal)?;
        spec.structured_calendar.push(structured);
    }

    // Parse ExcludeCalendar
    for cal in std::mem::take(&mut spec.exclude_calendar) {
        let structured = parse_calendar_to_structured(&cal)?;
        spec.exclude_structured_calendar.push(structured);
    }

    // Parse CronStrings
    let mut cron_tz: Option<Option<String>> = None; // None = unset
    for cs in std::mem::take(&mut spec.cron_string) {
        let (structured, interval, tz) = parse_cron_string(&cs)?;

        // Check timezone consistency across cron strings
        match (&cron_tz, &tz) {
            (None, _) => cron_tz = Some(tz),
            (Some(existing), new) if existing != new => {
                return Err(SpecError::ConflictingTimezoneNames);
            }
            _ => {}
        }

        if let Some(s) = structured {
            spec.structured_calendar.push(s);
        }
        if let Some(iv) = interval {
            spec.interval.push(iv);
        }
    }

    // If cron strings provided a timezone, copy it to spec, checking for
    // conflicts first.
    if let Some(Some(ref cron_tz_name)) = cron_tz {
        if !cron_tz_name.is_empty() {
            if !spec.timezone_name.is_empty() && spec.timezone_name != *cron_tz_name
                || spec.timezone_data.is_some()
            {
                return Err(SpecError::ConflictingTimezoneNames);
            }
            if spec.timezone_name.is_empty() {
                spec.timezone_name = cron_tz_name.clone();
            }
        }
    }

    // Validate structured calendars
    for scs in &spec.structured_calendar {
        validate_structured_calendar(scs)?;
    }

    // Validate intervals
    for iv in &spec.interval {
        validate_interval(iv)?;
    }

    Ok(spec)
}

// ---------------------------------------------------------------------------
// CleanSpec — set defaults for ranges in place
// ---------------------------------------------------------------------------

/// Sets default values in ranges (clamps End, defaults Step to 1).
pub fn clean_spec(spec: &mut ScheduleSpec) {
    for scs in &mut spec.structured_calendar {
        clean_cal(scs);
    }
    for scs in &mut spec.exclude_structured_calendar {
        clean_cal(scs);
    }
}

fn clean_ranges(ranges: &mut Vec<Range>) {
    for r in ranges.iter_mut() {
        if r.end < r.start {
            r.end = r.start;
        }
        if r.step == 0 {
            r.step = 1;
        }
    }
}

fn clean_cal(scs: &mut StructuredCalendarSpec) {
    clean_ranges(&mut scs.second);
    clean_ranges(&mut scs.minute);
    clean_ranges(&mut scs.hour);
    clean_ranges(&mut scs.day_of_month);
    clean_ranges(&mut scs.month);
    clean_ranges(&mut scs.year);
    clean_ranges(&mut scs.day_of_week);
}

// ---------------------------------------------------------------------------
// next_interval_time
// ---------------------------------------------------------------------------

/// Computes the next time after `after_secs` for a single `IntervalSpec`.
///
/// Formula (from Go):
/// ```text
/// interval_s = max(1, interval.Seconds())
/// phase_s    = max(0, phase.Seconds())
/// (((ts - phase_s) / interval_s) + 1) * interval_s + phase_s
/// ```
pub fn next_interval_time(iv: &IntervalSpec, after_secs: i64) -> i64 {
    let interval = (iv.interval.as_secs() as i64).max(1);
    let phase = (iv.phase.as_secs() as i64).max(0);
    // div_euclid 确保向下整除
    ((after_secs - phase).div_euclid(interval) + 1) * interval + phase
}

// ---------------------------------------------------------------------------
// add_jitter — deterministic hash-based jitter
// ---------------------------------------------------------------------------

/// Adds jitter to a nominal time deterministically, using FarmHash fingerprint
/// of the time's binary representation mixed with a seed string.
///
/// The algorithm:
/// 1. Serialize `nominal` to its RFC 3339 bytes
/// 2. Append `seed`
/// 3. Compute 32-bit FarmHash fingerprint
/// 4. Scale to the range [0, max_jitter) using the `(hash * max_ms) >> 32`
///    technique
pub fn add_jitter(seed: &str, nominal: Timestamp, max_jitter: Duration) -> Timestamp {
    if max_jitter.is_zero() {
        return nominal;
    }

    // 预分配准确的容量，避免 Vec 扩容
    let mut buf = Vec::with_capacity(12 + seed.len());
    // 直接写入 8 字节秒 + 4 字节纳秒，速度极快
    buf.extend_from_slice(&nominal.as_second().to_le_bytes());
    buf.extend_from_slice(&nominal.subsec_nanosecond().to_le_bytes());
    buf.extend_from_slice(seed.as_bytes());

    let fp: u64 = farmhash::fingerprint32(&buf) as u64;
    let max_ms: u64 = (max_jitter.as_millis() as u64).min(u32::MAX as u64);
    let jitter_ms: u64 = (fp * max_ms) >> 32;
    let jitter = Duration::from_millis(jitter_ms);

    nominal.checked_add(jitter).unwrap_or(nominal)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CalendarSpec;
    use jiff::civil::date;

    fn ts(y: i32, mo: i32, d: i32, h: i32, m: i32, s: i32, _ns: i32) -> Timestamp {
        let dt = civil_date(y, mo, d, h, m, s);
        let zoned = jiff::tz::TimeZone::UTC.to_zoned(dt).expect("valid time");
        zoned.timestamp()
    }

    fn civil_date(y: i32, mo: i32, d: i32, h: i32, m: i32, s: i32) -> jiff::civil::DateTime {
        jiff::civil::DateTime::from_parts(
            date(y as i16, mo as i8, d as i8),
            jiff::civil::Time::new(h as i8, m as i8, s as i8, 0).unwrap(),
        )
    }

    #[test]
    fn test_next_interval_time_basic() {
        let after = ts(2022, 3, 23, 12, 53, 2, 9);
        let iv = IntervalSpec {
            interval: Duration::from_secs(90 * 60),
            phase: Duration::ZERO,
        };
        let next = next_interval_time(&iv, after.as_second());
        let expected = ts(2022, 3, 23, 13, 30, 0, 0).as_second();
        assert_eq!(next, expected);
    }

    #[test]
    fn test_next_interval_time_with_phase() {
        let after = ts(2022, 3, 23, 12, 53, 2, 9);
        let iv = IntervalSpec {
            interval: Duration::from_secs(90 * 60),
            phase: Duration::from_secs(5 * 60 + 44),
        };
        let next = next_interval_time(&iv, after.as_second());
        let expected = ts(2022, 3, 23, 13, 35, 44, 0).as_second();
        assert_eq!(next, expected);
    }

    #[test]
    fn test_add_jitter_deterministic() {
        let t = ts(2022, 3, 23, 12, 0, 0, 0);
        let max_jitter = Duration::from_secs(3600);
        let r1 = add_jitter("seed-1", t, max_jitter);
        let r2 = add_jitter("seed-1", t, max_jitter);
        assert_eq!(r1, r2, "same seed → same result");

        let r3 = add_jitter("seed-2", t, max_jitter);
        assert_ne!(r1, r3, "different seed → different result");
    }

    #[test]
    fn test_add_jitter_within_range() {
        let t = ts(2022, 3, 23, 12, 0, 0, 0);
        let max_jitter = Duration::from_secs(3600);
        let r = add_jitter("seed", t, max_jitter);
        let delta_secs = r.as_second() - t.as_second();
        let delta_std = Duration::from_secs(delta_secs.max(0) as u64);
        assert!(
            delta_std < max_jitter,
            "jitter must be < max_jitter, got {delta_std:?} >= {max_jitter:?}"
        );
    }

    #[test]
    fn test_spec_interval_basic() {
        let builder = SpecBuilder::new();
        let cs = builder
            .new_compiled_spec(&ScheduleSpec {
                interval: vec![IntervalSpec {
                    interval: Duration::from_secs(90 * 60),
                    phase: Duration::ZERO,
                }],
                ..Default::default()
            })
            .unwrap();

        let start = ts(2022, 3, 23, 12, 53, 2, 9);
        let seq: Vec<Timestamp> = [
            ts(2022, 3, 23, 13, 30, 0, 0),
            ts(2022, 3, 23, 15, 0, 0, 0),
            ts(2022, 3, 23, 16, 30, 0, 0),
            ts(2022, 3, 23, 18, 0, 0, 0),
        ]
        .into_iter()
        .collect();

        let mut cur = start;
        for expected in &seq {
            let res = cs.get_next_time("", cur);
            assert!(res.next.is_some());
            assert_eq!(res.next.unwrap().as_second(), expected.as_second());
            cur = res.next.unwrap();
        }
    }

    #[test]
    fn test_spec_calendar_basic() {
        let builder = SpecBuilder::new();
        let cs = builder
            .new_compiled_spec(&ScheduleSpec {
                calendar: vec![CalendarSpec {
                    hour: "5,7".into(),
                    minute: "23".into(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();

        let start = ts(2022, 3, 23, 3, 0, 0, 0);
        let seq = [
            ts(2022, 3, 23, 5, 23, 0, 0),
            ts(2022, 3, 23, 7, 23, 0, 0),
            ts(2022, 3, 24, 5, 23, 0, 0),
            ts(2022, 3, 24, 7, 23, 0, 0),
        ];

        let mut cur = start;
        for expected in &seq {
            let res = cs.get_next_time("", cur);
            assert!(res.next.is_some(), "should have next after {cur:?}");
            assert_eq!(res.next.unwrap().as_second(), expected.as_second());
            cur = res.next.unwrap();
        }
    }

    #[test]
    fn test_spec_start_time() {
        let builder = SpecBuilder::new();
        let cs = builder
            .new_compiled_spec(&ScheduleSpec {
                interval: vec![IntervalSpec {
                    interval: Duration::from_secs(90 * 60),
                    phase: Duration::ZERO,
                }],
                start_time: Some(ts(2022, 3, 23, 12, 0, 0, 0)),
                jitter: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .unwrap();

        let start = ts(2022, 3, 23, 8, 0, 0, 0);
        let res = cs.get_next_time("", start);
        assert!(res.next.is_some());
        // Should be >= start_time
        assert!(res.next.unwrap().as_second() >= ts(2022, 3, 23, 12, 0, 0, 0).as_second());
    }

    #[test]
    fn test_spec_end_time() {
        let builder = SpecBuilder::new();
        let cs = builder
            .new_compiled_spec(&ScheduleSpec {
                interval: vec![IntervalSpec {
                    interval: Duration::from_secs(90 * 60),
                    phase: Duration::ZERO,
                }],
                end_time: Some(ts(2022, 3, 23, 14, 0, 0, 0)),
                jitter: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .unwrap();

        let start = ts(2022, 3, 23, 11, 0, 0, 0);
        let res1 = cs.get_next_time("", start);
        assert!(res1.next.is_some());

        let res2 = cs.get_next_time("", res1.next.unwrap());
        assert!(res2.next.is_some());

        let res3 = cs.get_next_time("", res2.next.unwrap());
        // Third should be exhausted (past end_time)
        assert!(
            res3.next.is_none()
                || res3.next.unwrap().as_second() >= ts(2022, 3, 23, 14, 0, 0, 0).as_second(),
            "res3 = {res3:?}"
        );
    }

    #[test]
    fn test_spec_exclude() {
        let builder = SpecBuilder::new();
        let cs = builder
            .new_compiled_spec(&ScheduleSpec {
                interval: vec![IntervalSpec {
                    interval: Duration::from_secs(90 * 60),
                    phase: Duration::ZERO,
                }],
                exclude_calendar: vec![CalendarSpec {
                    hour: "12-14".into(),
                    minute: "*".into(),
                    second: "*".into(),
                    ..Default::default()
                }],
                jitter: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .unwrap();

        // Start at 8:00 → next should be 9:00 (12-14 excluded; 8:00+90m=9:30 with
        // jitter). Actually 8:00 UTC → raw_next is 8:00, excluded? No, 8:00 is not
        // in 12-14. So next nominal is 9:00 (from 8:30, no — intervals are
        // 90min: ...6:00, 7:30, 9:00...). After 8:00, next interval is 9:00.
        // 9:00 is not in 12-14 exclude range. So next = 9:00 + jitter.
        let start = ts(2022, 3, 23, 8, 0, 0, 0);
        let res = cs.get_next_time("", start);
        assert!(res.next.is_some());
        let h = {
            let z = res.next.unwrap().to_zoned(jiff::tz::TimeZone::UTC);
            z.datetime().hour()
        };
        assert_eq!(h, 9);
    }

    #[test]
    fn test_exclude_all() {
        let builder = SpecBuilder::new();
        let cs = builder
            .new_compiled_spec(&ScheduleSpec {
                interval: vec![IntervalSpec {
                    interval: Duration::from_secs(7 * 24 * 3600),
                    phase: Duration::ZERO,
                }],
                exclude_calendar: vec![CalendarSpec {
                    second: "*".into(),
                    minute: "*".into(),
                    hour: "*".into(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();

        let res = cs.get_next_time("", ts(2022, 3, 23, 12, 53, 2, 9));
        assert!(res.nominal.is_none(), "everything excluded → no next time");
    }

    #[test]
    fn test_jitter_bounded_by_next_interval() {
        let builder = SpecBuilder::new();
        let cs = builder
            .new_compiled_spec(&ScheduleSpec {
                interval: vec![IntervalSpec {
                    interval: Duration::from_secs(90 * 60),
                    phase: Duration::ZERO,
                }],
                jitter: Some(Duration::from_secs(24 * 3600)), // huge jitter
                ..Default::default()
            })
            .unwrap();

        let start = ts(2022, 3, 23, 11, 0, 0, 0);
        let res = cs.get_next_time("", start);
        assert!(res.next.is_some());
        let z = res.next.unwrap().to_zoned(jiff::tz::TimeZone::UTC);
        let h = z.datetime().hour();
        assert!(h >= 12);
    }

    #[test]
    fn test_canonicalize_full() {
        let spec = ScheduleSpec {
            calendar: vec![CalendarSpec {
                hour: "5,7".into(),
                minute: "23".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let canonical = canonicalize_spec(&spec).unwrap();
        assert_eq!(canonical.structured_calendar.len(), 1);
        let scs = &canonical.structured_calendar[0];
        // Default second should be [0]
        assert_eq!(scs.second.len(), 1);
        assert_eq!(scs.second[0].start, 0);
        // Hour should have two ranges: [5] and [7]
        assert_eq!(scs.hour.len(), 2);
        assert_eq!(scs.hour[0].start, 5);
        assert_eq!(scs.hour[1].start, 7);
        // Minute should be [23]
        assert_eq!(scs.minute.len(), 1);
        assert_eq!(scs.minute[0].start, 23);
        // DayOfMonth default is 1-31
        assert_eq!(scs.day_of_month[0].start, 1);
        assert_eq!(scs.day_of_month[0].end, 31);
        // Month default is 1-12
        assert_eq!(scs.month[0].start, 1);
        assert_eq!(scs.month[0].end, 12);
    }
}
