//! 日历规格解析与编译型日历匹配。
//!
//! 移植自 Temporal 调度器的 `calendar.go`（~561 行）。
//!
//! # 模块职责
//!
//! 1. **编译**: 将 `StructuredCalendarSpec` 编译为 `CompiledCalendar`
//!    （使用 `BitMatcher` 和 `YearMatcher` 实现 O(1) 字段匹配）
//! 2. **搜索**: `CompiledCalendar::next(after)` — 找到 >= after 的最早匹配时间
//!    （手动进位迭代: 秒→分→时→日→月→年，正确处理 DST）
//! 3. **解析**: Cron 字符串 → `StructuredCalendarSpec`；字段字符串 → `Range[]`
//!
//! # 核心算法: next() 搜索
//!
//! ```text
//! 输入: after (Timestamp)
//! 1. 转换到目标时区，提取 y/mo/d/h/m/s，s++
//! 2. 检测 DST 回退重复小时
//! 3. 循环:
//!    归一化进位 (s→m→h→d→mo→y)
//!    从外到内匹配: year → month → day+weekday → hour → minute → second
//!    不匹配→跳过到下一个候选
//!    匹配→构建 Zoned 时间，检查 DST 间隙
//! 4. 返回匹配时间 + dst_offset
//! ```
//!
//! # BitMatcher 设计
//!
//! 将 [0..63] 范围的值映射到 `u64` 的对应位:
//! - 匹配 {1,3,4,5} → `0b...00111010` → O(1) 查找
//! - 对于 0-59 的秒/分、0-23 的时、1-31 的日，u64 完全够用
//! - 年份单独使用 `YearMatcher`（支持 "all" 或稀疏值列表）

use std::time::Duration;
use std::{str::FromStr, time};

use jiff::{
    civil::{Date, DateTime, Time},
    tz::TimeZone,
    Timestamp,
};

use crate::error::SpecError;
use crate::types::{
    CalendarSpec, IntervalSpec, Range, StructuredCalendarSpec, MAX_CALENDAR_YEAR,
    MAX_COMMENT_LEN, MIN_CALENDAR_YEAR,
};

// ---------------------------------------------------------------------------
// compiledCalendar — a compiled calendar spec with fast matchers
// ---------------------------------------------------------------------------

/// A compiled calendar spec.  Each field has a predicate function; a time
/// matches when *all* fields return `true`.
///
/// Months are 1-based (January = 1).  Days of week are 0-based (Sunday = 0).
/// All matching is done at 1-second resolution.
#[derive(Clone)]
pub struct CompiledCalendar {
    /// Time zone that this calendar spec is interpreted in.
    tz: TimeZone,

    /// Matching predicates for each field.
    year:       YearMatcher,
    month:      BitMatcher,
    day_of_month: BitMatcher,
    day_of_week:  BitMatcher,
    hour:       BitMatcher,
    minute:     BitMatcher,
    second:     BitMatcher,
}

// ---------------------------------------------------------------------------
// BitMatcher — fast O(1) lookup using a 64-bit bitset
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct BitMatcher(u64);

impl BitMatcher {
    fn new(ranges: &[Range], default_all: bool) -> Self {
        if ranges.is_empty() && default_all {
            return Self(u64::MAX);
        }
        let mut bits: u64 = 0;
        iterate_ranges(ranges, |i| {
            if (0..64).contains(&i) {
                bits |= 1u64 << i;
            }
        });
        Self(bits)
    }

    fn matches(&self, v: i32) -> bool {
        if v < 0 || v >= 64 {
            return false;
        }
        (self.0 & (1u64 << v)) != 0
    }
}

// ---------------------------------------------------------------------------
// YearMatcher — handles the sparse year case with a small vec
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum YearMatcher {
    All,
    Values(Vec<i16>),
}

impl YearMatcher {
    fn new(ranges: &[Range]) -> Self {
        if ranges.is_empty() {
            // special case for year: empty range list means "all"
            return Self::All;
        }
        let mut values: Vec<i16> = Vec::new();
        iterate_ranges(ranges, |i| values.push(i as i16));
        Self::Values(values)
    }

    fn matches(&self, v: i32) -> bool {
        match self {
            Self::All => true,
            Self::Values(vs) => vs.contains(&(v as i16)),
        }
    }
}

// ---------------------------------------------------------------------------
// iterate_ranges — expands ranges and calls `f` for each matching value
// ---------------------------------------------------------------------------

fn iterate_ranges(ranges: &[Range], mut f: impl FnMut(i32)) {
    for r in ranges {
        let start = r.start;
        let end = r.effective_end();
        let step = r.effective_step();
        let mut v = start;
        while v <= end {
            f(v);
            v += step;
            // guard against infinite loops from step <= 0 (validated earlier)
            if step <= 0 {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CompiledCalendar methods
// ---------------------------------------------------------------------------

impl CompiledCalendar {
    /// Build a compiled calendar from a structured spec + timezone.
    pub fn new(cal: &StructuredCalendarSpec, tz: TimeZone) -> Self {
        Self {
            year: YearMatcher::new(&cal.year),
            month: BitMatcher::new(&cal.month, false),
            day_of_month: BitMatcher::new(&cal.day_of_month, false),
            day_of_week: BitMatcher::new(&cal.day_of_week, false),
            hour: BitMatcher::new(&cal.hour, false),
            minute: BitMatcher::new(&cal.minute, false),
            second: BitMatcher::new(&cal.second, false),
            tz,
        }
    }

    /// Returns `true` if the given timestamp matches this calendar spec.
    pub fn matches(&self, ts: Timestamp) -> bool {
        let zoned = ts.to_zoned(self.tz.clone());
        let dt = zoned.datetime();
        self.year.matches(dt.year() as i32)
            && self.month.matches(dt.month() as i32)
            && self.day_of_month.matches(dt.day() as i32)
            && self
                .day_of_week
                .matches(dt.weekday().to_sunday_zero_offset() as i32)
            && self.hour.matches(dt.hour() as i32)
            && self.minute.matches(dt.minute() as i32)
            && self.second.matches(dt.second() as i32)
    }

    /// 返回此日历规格中 ≥ `after` 的最早匹配时间。
    ///
    /// 如果没有更多匹配时间（超过 MAX_CALENDAR_YEAR 或所有时间均被排除），返回 `None`。
    /// 分辨率: 1 秒。
    ///
    /// 实现: 转换到目标时区，提取 y/mo/d/h/m/s 分量，从 after+1s 开始
    /// 手动进位迭代（秒→分→时→日→月→年），从外到内匹配，正确处理 DST。
    pub fn next(&self, after: Timestamp) -> Option<Timestamp> {
        self.next_impl(after)
    }

    fn next_impl(&self, after: Timestamp) -> Option<Timestamp> {
        let zoned = after.to_zoned(self.tz.clone());
        let dt = zoned.datetime();
        let y = dt.year() as i32;
        let mo = dt.month() as i32;
        let d = dt.day() as i32;
        let h = dt.hour() as i32;
        let m = dt.minute() as i32;
        // s = dt.second() + 1  (we want *after* after)
        let s = dt.second() as i32 + 1;

        // Detect DST repeated hour (fall-back): if we subtract 1 hour and still
        // get the same hour, we're in the second occurrence.
        let dst_offset = {
            let minus_1h = after
                .checked_sub(time::Duration::from_secs(3600))
                .unwrap_or(after);
            let minus_1h_zoned = minus_1h.to_zoned(self.tz.clone());
            if minus_1h_zoned.datetime().hour() == dt.hour() as i8 {
                Some(3600i64) // 1 hour in seconds
            } else {
                None
            }
        };

        self.search(y, mo, d, h, m, s, dst_offset)
    }

    #[allow(clippy::too_many_arguments)]
    fn search(
        &self,
        mut y: i32,
        mut mo: i32,
        mut d: i32,
        mut h: i32,
        mut m: i32,
        mut s: i32,
        mut dst_offset: Option<i64>,
    ) -> Option<Timestamp> {
        // Normalize seconds
        if s >= 60 {
            m += 1;
            s = 0;
        }
        // Normalize minutes
        if m >= 60 {
            let prev_dt = civil_date(y, mo, d, h, 0, 0);
            h += 1;
            m = 0;
            let next_dt = civil_date(y, mo, d, h, 0, 0);
            if dst_offset.is_none() {
                // Compare the true elapsed time by converting to timestamps.
                let prev_ts = self.tz.to_zoned(prev_dt).ok().map(|z| z.timestamp());
                let next_ts = self.tz.to_zoned(next_dt).ok().map(|z| z.timestamp());
                if let (Some(p), Some(n)) = (prev_ts, next_ts) {
                    let gap_secs = n.as_second() - p.as_second();
                    if gap_secs > 3600 {
                        // We skipped over a DST repeated hour. Back up and try
                        // with offset.
                        h -= 1;
                        dst_offset = Some(3600);
                    } else {
                        dst_offset = None;
                    }
                } else {
                    dst_offset = None;
                }
            } else {
                dst_offset = None;
            }
        }
        // Normalize hours
        if h >= 24 {
            d += 1;
            h = 0;
        }
        // Normalize day
        if d > days_in_month(mo, y) {
            mo += 1;
            d = 1;
        }
        // Normalize month
        if mo > 12 {
            y += 1;
            mo = 1;
        }
        if y > MAX_CALENDAR_YEAR {
            return None;
        }

        // Check year (skip to next year if it doesn't match)
        if !self.year.matches(y) {
            return self.search(y + 1, 1, 1, 0, 0, 0, None);
        }
        // Check month (advance within year)
        if !self.month.matches(mo) {
            let next_mo = mo + 1;
            if next_mo > 12 {
                return self.search(y + 1, 1, 1, 0, 0, 0, None);
            }
            return self.search(y, next_mo, 1, 0, 0, 0, None);
        }
        // Check day (advance within month)
        if !self.day_of_month.matches(d)
            || !self
                .day_of_week
                .matches(date_weekday(y, mo, d, self.tz.clone()) as i32)
        {
            let next_d = d + 1;
            if next_d > days_in_month(mo, y) {
                let next_mo = mo + 1;
                if next_mo > 12 {
                    return self.search(y + 1, 1, 1, 0, 0, 0, None);
                }
                return self.search(y, next_mo, 1, 0, 0, 0, None);
            }
            return self.search(y, mo, next_d, 0, 0, 0, None);
        }
        // Check hour
        if !self.hour.matches(h) {
            let next_h = h + 1;
            if next_h >= 24 {
                let next_d = d + 1;
                if next_d > days_in_month(mo, y) {
                    let next_mo = mo + 1;
                    if next_mo > 12 {
                        return self.search(y + 1, 1, 1, 0, 0, 0, None);
                    }
                    return self.search(y, next_mo, 1, 0, 0, 0, None);
                }
                return self.search(y, mo, next_d, 0, 0, 0, None);
            }
            return self.search(y, mo, d, next_h, 0, 0, None);
        }
        // Check minute
        if !self.minute.matches(m) {
            let next_m = m + 1;
            if next_m >= 60 {
                let next_h = h + 1;
                if next_h >= 24 {
                    let next_d = d + 1;
                    if next_d > days_in_month(mo, y) {
                        let next_mo = mo + 1;
                        if next_mo > 12 {
                            return self.search(y + 1, 1, 1, 0, 0, 0, None);
                        }
                        return self.search(y, next_mo, 1, 0, 0, 0, None);
                    }
                    return self.search(y, mo, next_d, 0, 0, 0, None);
                }
                return self.search(y, mo, d, next_h, 0, 0, None);
            }
            return self.search(y, mo, d, h, next_m, 0, None);
        }
        // Check second
        if !self.second.matches(s) {
            let next_s = s + 1;
            if next_s >= 60 {
                let next_m = m + 1;
                if next_m >= 60 {
                    let next_h = h + 1;
                    if next_h >= 24 {
                        let next_d = d + 1;
                        if next_d > days_in_month(mo, y) {
                            let next_mo = mo + 1;
                            if next_mo > 12 {
                                return self.search(y + 1, 1, 1, 0, 0, 0, None);
                            }
                            return self.search(y, next_mo, 1, 0, 0, 0, None);
                        }
                        return self.search(y, mo, next_d, 0, 0, 0, None);
                    }
                    return self.search(y, mo, d, next_h, 0, 0, None);
                }
                return self.search(y, mo, d, h, next_m, 0, None);
            }
            return self.search(y, mo, d, h, m, next_s, None);
        }

        // Everything matches — build the resulting Zoned time.
        let civil_dt = civil_date(y, mo, d, h, m, s);
        let zoned = match self.tz.to_zoned(civil_dt) {
            Ok(z) => z,
            Err(_) => {
                // Non-existent time (DST spring-forward gap).
                // Skip to next hour.
                return self.search(y, mo, d, h + 1, 0, 0, None);
            }
        };
        // DST gap check: if the resulting hour differs from what we intended,
        // skip to the next hour.
        if zoned.datetime().hour() != h as i8 {
            return self.search(y, mo, d, h + 1, 0, 0, None);
        }

        let ts = zoned.timestamp();
        // If we were in a DST repeated hour, add the extra offset.
        if let Some(off) = dst_offset {
            return Some(
                ts.checked_add(time::Duration::from_secs(off as u64))
                    .expect("dst-offset timestamp overflow"),
            );
        }
        Some(ts)
    }
}

// ---------------------------------------------------------------------------
// Helper: build a civil::DateTime from components
// ---------------------------------------------------------------------------

fn civil_date(y: i32, mo: i32, d: i32, h: i32, m: i32, s: i32) -> DateTime {
    let yi: i16 = y.try_into().expect("year fits in i16");
    let date = Date::new(yi, mo as i8, d as i8).expect("valid date");
    let time = Time::new(h as i8, m as i8, s as i8, 0).expect("valid time");
    DateTime::from_parts(date, time)
}

/// Compute the day-of-week for a (y, mo, d) in the given timezone.
/// Returns 0 = Sunday, …, 6 = Saturday.
/// Falls back to UTC when the date is not representable in the timezone.
fn date_weekday(y: i32, mo: i32, d: i32, tz: TimeZone) -> u8 {
    let yi: i16 = y.try_into().expect("year fits in i16");
    let date = Date::new(yi, mo as i8, d as i8).expect("valid date");
    let civil_dt = DateTime::from_parts(date, Time::midnight());
    match tz.to_zoned(civil_dt) {
        Ok(z) => z.weekday().to_sunday_zero_offset() as u8,
        Err(_) => date.weekday().to_sunday_zero_offset() as u8,
    }
}

// ---------------------------------------------------------------------------
// calendar → structured parsing
// ---------------------------------------------------------------------------

/// Parses a `CalendarSpec` (crontab-like string fields) into a
/// `StructuredCalendarSpec`.
pub fn parse_calendar_to_structured(cal: &CalendarSpec) -> Result<StructuredCalendarSpec, SpecError> {
    let mut errs: Vec<String> = Vec::new();

    let mut make_range_or_nil =
        |s: &str, field: &str, def: &str, min_val: i32, max_val: i32, mode: ParseMode| {
            match make_range(s, field, def, min_val, max_val, mode) {
                Ok(r) => r,
                Err(e) => {
                    errs.push(e.to_string());
                    Vec::new()
                }
            }
        };

    let ss = StructuredCalendarSpec {
        second: make_range_or_nil(&cal.second, "Second", "0", 0, 59, ParseMode::Int),
        minute: make_range_or_nil(&cal.minute, "Minute", "0", 0, 59, ParseMode::Int),
        hour: make_range_or_nil(&cal.hour, "Hour", "0", 0, 23, ParseMode::Int),
        day_of_week: make_range_or_nil(
            &cal.day_of_week,
            "DayOfWeek",
            "*",
            0,
            7,
            ParseMode::Dow,
        ),
        day_of_month: make_range_or_nil(
            &cal.day_of_month,
            "DayOfMonth",
            "*",
            1,
            31,
            ParseMode::Int,
        ),
        month: make_range_or_nil(&cal.month, "Month", "*", 1, 12, ParseMode::Month),
        year: make_range_or_nil(
            &cal.year,
            "Year",
            "*",
            MIN_CALENDAR_YEAR,
            MAX_CALENDAR_YEAR,
            ParseMode::Year,
        ),
        comment: cal.comment.clone(),
    };

    if !errs.is_empty() {
        return Err(SpecError::InvalidCalendarSpec(errs.join(", ")));
    }
    Ok(ss)
}

// ---------------------------------------------------------------------------
// Cron string parsing
// ---------------------------------------------------------------------------

/// Parse a cron string into a structured calendar, an interval, and a
/// timezone name.  Returns `(Option<structured>, Option<interval>, tz_name)`.
pub fn parse_cron_string(
    c: &str,
) -> Result<
    (
        Option<StructuredCalendarSpec>,
        Option<IntervalSpec>,
        Option<String>,
    ),
    SpecError,
> {
    let mut c = c.trim().to_string();
    let mut tz_name: Option<String> = None;
    let mut comment: String = String::new();

    // Split out timezone prefix
    if let Some(rest) = c.strip_prefix("TZ=").or_else(|| c.strip_prefix("CRON_TZ=")) {
        let (tz_val, rest) = rest
            .split_once(' ')
            .ok_or(SpecError::CronStringMissingFields)?;
        tz_name = Some(tz_val.to_string());
        c = rest.to_string();
    }

    // Split out comment
    if let Some((rest, comm)) = c.split_once('#') {
        // Work around borrow checker: use the string slices before reassigning c.
        let rest = rest.trim().to_string();
        comment = comm.trim().to_string();
        c = rest;
    }

    // Handle @every intervals
    if c.starts_with("@every") {
        let iv = parse_cron_interval(&c)?;
        return Ok((None, Some(iv), None));
    }

    // Handle @hourly, @daily, etc.
    c = handle_predefined_cron_strings(&c);

    // Split fields (5-7 space-separated)
    let fields: Vec<&str> = c.split_whitespace().collect();
    let mut cal = CalendarSpec {
        comment,
        ..Default::default()
    };

    match fields.len() {
        5 => {
            cal.minute = fields[0].to_string();
            cal.hour = fields[1].to_string();
            cal.day_of_month = fields[2].to_string();
            cal.month = fields[3].to_string();
            cal.day_of_week = fields[4].to_string();
        }
        6 => {
            cal.minute = fields[0].to_string();
            cal.hour = fields[1].to_string();
            cal.day_of_month = fields[2].to_string();
            cal.month = fields[3].to_string();
            cal.day_of_week = fields[4].to_string();
            cal.year = fields[5].to_string();
        }
        7 => {
            cal.second = fields[0].to_string();
            cal.minute = fields[1].to_string();
            cal.hour = fields[2].to_string();
            cal.day_of_month = fields[3].to_string();
            cal.month = fields[4].to_string();
            cal.day_of_week = fields[5].to_string();
            cal.year = fields[6].to_string();
        }
        _ => return Err(SpecError::CronStringFieldCount),
    }

    let structured = parse_calendar_to_structured(&cal)?;
    Ok((Some(structured), None, tz_name))
}

fn parse_cron_interval(c: &str) -> Result<IntervalSpec, SpecError> {
    let (_, rest) = c
        .split_once(' ')
        .ok_or(SpecError::CronStringMissingInterval)?;
    let (interval_str, phase_str) = match rest.split_once('/') {
        Some((i, p)) => (i, Some(p)),
        None => (rest, None),
    };

    let interval = parse_duration(interval_str)
        .map_err(|e| SpecError::RangeError {
            field: "Interval".into(),
            detail: e.to_string(),
        })?;

    let phase = match phase_str {
        Some(p) => parse_duration(p).map_err(|e| SpecError::RangeError {
            field: "Phase".into(),
            detail: e.to_string(),
        })?,
        None => Duration::ZERO,
    };

    Ok(IntervalSpec { interval, phase })
}

fn handle_predefined_cron_strings(c: &str) -> String {
    match c {
        "@yearly" | "@annually" => "0 0 1 1 *".to_string(),
        "@monthly" => "0 0 1 * *".to_string(),
        "@weekly" => "0 0 * * 0".to_string(),
        "@daily" | "@midnight" => "0 0 * * *".to_string(),
        "@hourly" => "0 * * * *".to_string(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Duration parsing — supports "3h", "90m", "5s", "1h30m", etc.
// ---------------------------------------------------------------------------

/// Parse a Go-style duration string (e.g. "90m", "3h", "5m44s").
pub fn parse_duration(s: &str) -> Result<Duration, SpecError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(SpecError::Internal("empty duration".into()));
    }

    // Try standard humantime / Go duration format.
    // Go durations can be like "90m", "3h", "1h30m", "5m44s", "1.5h", etc.
    // We support integer + unit, combined additively.
    let mut total = Duration::ZERO;
    let mut num_buf = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            num_buf.push(ch);
        } else {
            let unit = ch;
            let val = if let Ok(f) = f64::from_str(&num_buf) {
                f
            } else {
                return Err(SpecError::Internal(format!("invalid number: {num_buf}")));
            };
            num_buf.clear();

            match unit {
                'h' => {
                    total += Duration::from_secs_f64(val * 3600.0);
                }
                'm' => {
                    total += Duration::from_secs_f64(val * 60.0);
                }
                's' => {
                    total += Duration::from_secs_f64(val);
                }
                'u' | 'µ' => {
                    total += Duration::from_micros(val as u64);
                }
                'n' => {
                    total += Duration::from_nanos(val as u64);
                }
                _ => {
                    return Err(SpecError::Internal(format!(
                        "unknown duration unit: {unit}"
                    )));
                }
            }
        }
    }

    if !num_buf.is_empty() {
        return Err(SpecError::Internal(format!(
            "missing unit in duration: {s}"
        )));
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// Range parsing
// ---------------------------------------------------------------------------

/// Parsing mode for range values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseMode {
    /// Decimal integers only.
    Int,
    /// Like `Int` but returns an empty vec for `*` (special year handling).
    Year,
    /// Accepts month name prefixes (≥ 3 letters).
    Month,
    /// Accepts day-of-week name prefixes (≥ 2 letters).
    Dow,
}

/// Month names (index 0 = January).
const MONTH_STRINGS: [&str; 12] = [
    "january", "february", "march", "april", "may", "june", "july", "august", "september",
    "october", "november", "december",
];

/// Day-of-week names (index 0 = Sunday).
const DOW_STRINGS: [&str; 7] = [
    "sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
];

/// Parse a range string like `"1-5/2,8-11"` into a list of `Range`.
///
/// Syntax:
///   `*`        matches always
///   `x`        matches when the field equals x
///   `x-z`      matches when the field is between x and z inclusive
///   `x-z/y`    like `x-z` but stepping by y
///   `x/y`      matches x to max, stepping by y
///   `j,k,l`    multi-value (comma-separated)
pub fn make_range(
    s: &str,
    field: &str,
    def: &str,
    min_val: i32,
    max_val: i32,
    parse_mode: ParseMode,
) -> Result<Vec<Range>, SpecError> {
    let s = s.trim();
    let s = if s.is_empty() { def } else { s };

    if s == "*" && parse_mode == ParseMode::Year {
        // Special case for year: empty list means "all".
        return Ok(Vec::new());
    }

    let mut ranges: Vec<Range> = Vec::new();

    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let (base, step) = match part.split_once('/') {
            Some((b, st)) => {
                if st.contains('/') {
                    return Err(SpecError::RangeError {
                        field: field.into(),
                        detail: format!("{field} has too many slashes"),
                    });
                }
                if st.is_empty() {
                    return Err(SpecError::RangeError {
                        field: field.into(),
                        detail: format!("{field} missing step value"),
                    });
                }
                let step_val = st
                    .parse::<i32>()
                    .map_err(|_| SpecError::RangeError {
                        field: field.into(),
                        detail: "invalid step".into(),
                    })?;
                if step_val < 1 {
                    return Err(SpecError::RangeError {
                        field: field.into(),
                        detail: format!("{field} has invalid Step"),
                    });
                }
                (b, step_val)
            }
            None => (part, 1),
        };

        let has_step = part.contains('/');

        let (start, end) = if base == "*" {
            (min_val, max_val)
        } else if base.contains('-') {
            if base.matches('-').count() > 1 {
                return Err(SpecError::RangeError {
                    field: field.into(),
                    detail: format!("{field} has too many dashes"),
                });
            }
            let (s1, e1): (&str, &str) = base.split_once('-').expect("has dash");
            let st = parse_value(s1, min_val, max_val, parse_mode).map_err(|_| {
                SpecError::RangeError {
                    field: field.into(),
                    detail: format!("{field} Start is not in range [{min_val}-{max_val}]"),
                }
            })?;
            let en = parse_value(e1, st, max_val, parse_mode).map_err(|_| {
                SpecError::RangeError {
                    field: field.into(),
                    detail: format!(
                        "{field} End is before Start or not in range [{min_val}-{max_val}]"
                    ),
                }
            })?;
            (st, en)
        } else {
            let st = parse_value(base, min_val, max_val, parse_mode).map_err(|_| {
                SpecError::RangeError {
                    field: field.into(),
                    detail: format!("{field} is not in range [{min_val}-{max_val}]"),
                }
            })?;
            let en = if has_step { max_val } else { st };
            (st, en)
        };

        // Handle Sunday=7 → Sunday=0 conversion for DayOfWeek
        if parse_mode == ParseMode::Dow && end == 7 {
            // check if 7 is actually included
            let seven_included = (7 - start) % step == 0;
            if seven_included && (step > 1 || (step == 1 && start > 1)) {
                ranges.push(Range::new(0));
                if start == 7 {
                    continue;
                }
            }
            let end = if start == 7 && end == 7 { 6 } else { 6 };
            add_range(&mut ranges, start, if seven_included { end } else { end }, step);
        } else {
            add_range(&mut ranges, start, end, step);
        }
    }

    Ok(ranges)
}

fn add_range(ranges: &mut Vec<Range>, start: i32, end: i32, step: i32) {
    let end_enc = if start == end { 0 } else { end };
    let step_enc = if step == 1 { 0 } else { step };
    ranges.push(Range {
        start,
        end: end_enc,
        step: step_enc,
    });
}

/// Parse a single value: integer, month name prefix, or day-of-week name prefix.
fn parse_value(s: &str, min: i32, max: i32, mode: ParseMode) -> Result<i32, SpecError> {
    let s = s.trim();

    match mode {
        ParseMode::Month => {
            if s.len() >= 3 {
                let s = s.to_lowercase();
                for (i, &month) in MONTH_STRINGS.iter().enumerate() {
                    if month.starts_with(&s) {
                        let v = (i + 1) as i32;
                        if v < min || v > max {
                            return Err(SpecError::RangeError {
                                field: "Month".into(),
                                detail: "out of range".into(),
                            });
                        }
                        return Ok(v);
                    }
                }
            }
        }
        ParseMode::Dow => {
            if s.len() >= 2 {
                let s = s.to_lowercase();
                for (i, &dow) in DOW_STRINGS.iter().enumerate() {
                    if dow.starts_with(&s) {
                        let v = i as i32;
                        if v < min || v > max {
                            return Err(SpecError::RangeError {
                                field: "DayOfWeek".into(),
                                detail: "out of range".into(),
                            });
                        }
                        return Ok(v);
                    }
                }
            }
        }
        _ => {}
    }

    let v: i32 = s.parse().map_err(|_| SpecError::RangeError {
        field: String::new(),
        detail: format!("not an integer: {s}"),
    })?;

    if v < min || v > max {
        return Err(SpecError::RangeError {
            field: String::new(),
            detail: format!("{v} out of range [{min}-{max}]"),
        });
    }

    Ok(v)
}

// ---------------------------------------------------------------------------
// Calendar validation
// ---------------------------------------------------------------------------

/// Validate a `StructuredCalendarSpec`.
pub fn validate_structured_calendar(scs: &StructuredCalendarSpec) -> Result<(), SpecError> {
    let mut errs: Vec<String> = Vec::new();

    let mut check = |ranges: &[Range], field: &str, min_val: i32, max_val: i32| {
        for r in ranges {
            if r.start < min_val || r.start > max_val {
                errs.push(format!(
                    "{field} Start is not in range [{min_val}-{max_val}]"
                ));
            }
            if r.end != 0 && (r.end < r.start || r.end > max_val) {
                errs.push(format!(
                    "{field} End is before Start or not in range [{min_val}-{max_val}]"
                ));
            }
            if r.step < 0 {
                errs.push(format!("{field} has invalid Step"));
            }
        }
    };

    check(&scs.second, "Second", 0, 59);
    check(&scs.minute, "Minute", 0, 59);
    check(&scs.hour, "Hour", 0, 23);
    check(&scs.day_of_month, "DayOfMonth", 1, 31);
    check(&scs.month, "Month", 1, 12);
    check(&scs.year, "Year", MIN_CALENDAR_YEAR, MAX_CALENDAR_YEAR);
    check(&scs.day_of_week, "DayOfWeek", 0, 6);

    if scs.comment.len() > MAX_COMMENT_LEN {
        errs.push("comment is too long".into());
    }

    if !errs.is_empty() {
        return Err(SpecError::InvalidCalendarSpec(errs.join(", ")));
    }
    Ok(())
}

/// Validate an `IntervalSpec`.
pub fn validate_interval(iv: &IntervalSpec) -> Result<(), SpecError> {
    if iv.interval < Duration::from_secs(1) {
        return Err(SpecError::IntervalTooSmall);
    }
    if iv.phase > iv.interval {
        return Err(SpecError::PhaseExceedsInterval);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_leap_year(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn days_in_month(mo: i32, y: i32) -> i32 {
    match mo {
        1 => 31,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        3 => 31,
        4 => 30,
        5 => 31,
        6 => 30,
        7 => 31,
        8 => 31,
        9 => 30,
        10 => 31,
        11 => 30,
        12 => 31,
        _ => 30, // shouldn't happen
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_in_month() {
        assert_eq!(days_in_month(1, 2022), 31);
        assert_eq!(days_in_month(2, 2022), 28);
        assert_eq!(days_in_month(2, 2020), 29);
        assert_eq!(days_in_month(4, 2022), 30);
    }

    #[test]
    fn test_parse_predefined_cron() {
        assert_eq!(handle_predefined_cron_strings("@hourly"), "0 * * * *");
        assert_eq!(
            handle_predefined_cron_strings("@daily"),
            "0 0 * * *"
        );
        assert_eq!(
            handle_predefined_cron_strings("@weekly"),
            "0 0 * * 0"
        );
        assert_eq!(
            handle_predefined_cron_strings("@monthly"),
            "0 0 1 * *"
        );
        assert_eq!(
            handle_predefined_cron_strings("@yearly"),
            "0 0 1 1 *"
        );
        assert_eq!(
            handle_predefined_cron_strings("@annually"),
            "0 0 1 1 *"
        );
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(
            parse_duration("90m").unwrap(),
            Duration::from_secs(90 * 60)
        );
        assert_eq!(
            parse_duration("3h").unwrap(),
            Duration::from_secs(3 * 3600)
        );
        assert_eq!(
            parse_duration("5m44s").unwrap(),
            Duration::from_secs(5 * 60 + 44)
        );
    }

    #[test]
    fn test_make_range_basic() {
        let r = make_range("5,7", "Hour", "0", 0, 23, ParseMode::Int).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].start, 5);
        assert_eq!(r[1].start, 7);
    }

    #[test]
    fn test_make_range_default() {
        let r = make_range("*", "Month", "*", 1, 12, ParseMode::Int).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].start, 1);
        // end is 0 (encoding for "same as start")
        // step is 0 (encoding for 1)
    }

    #[test]
    fn test_bit_matcher() {
        let ranges = vec![
            Range::new(1),
            Range {
                start: 3,
                end: 5,
                step: 0,
            },
        ];
        let m = BitMatcher::new(&ranges, false);
        assert!(m.matches(1));
        assert!(!m.matches(2));
        assert!(m.matches(3));
        assert!(m.matches(4));
        assert!(m.matches(5));
        assert!(!m.matches(6));
    }
}
