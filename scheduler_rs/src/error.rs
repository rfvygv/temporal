//! 调度器规格引擎的错误类型。
//!
//! 使用 `thiserror` 派生宏实现 `std::error::Error` trait，
//! 对应 Go 代码中各个包里的 `error` 和 `fmt.Errorf` 调用。

use thiserror::Error;

// ---------------------------------------------------------------------------
// SpecError — 调度规格解析/编译错误
// ---------------------------------------------------------------------------

/// 调度规格在解析、验证和编译过程中可能出现的所有错误。
///
/// 对应 Go 代码中的：
/// - `errConflictingTimezoneNames`
/// - `errors.New("invalid calendar spec: ...")`
/// - `errors.New("interval is too small")`
/// - 各种 `fmt.Errorf` 返回的错误
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum SpecError {
    /// 日历规格无效。包含详细的错误描述（多个错误用逗号分隔）。
    /// 对应 Go: `errors.New("invalid calendar spec: " + strings.Join(errs, ", "))`
    #[error("invalid calendar spec: {0}")]
    InvalidCalendarSpec(String),

    /// 多个 Cron 字符串之间的时区名不一致。
    /// 对应 Go: `errConflictingTimezoneNames`
    #[error("conflicting timezone names")]
    ConflictingTimezoneNames,

    /// 间隔太小（最小值为 1 秒）。
    /// 对应 Go: `errors.New("interval is too small")`
    #[error("interval is too small (minimum 1 second)")]
    IntervalTooSmall,

    /// 相位值为负数。
    /// 对应 Go: `errors.New("phase is negative")`
    #[error("phase is negative")]
    PhaseNegative,

    /// 相位大于间隔（Phase 必须 < Interval）。
    /// 对应 Go: `errors.New("phase cannot be greater than Interval")`
    #[error("phase cannot be greater than interval")]
    PhaseExceedsInterval,

    /// Cron 字符串有 TZ= 前缀但缺少后续字段。
    /// 对应 Go: `errors.New("CronString has time zone but missing fields")`
    #[error("cron string has time zone but missing fields")]
    CronStringMissingFields,

    /// Cron 字符串字段数量不是 5-7。
    /// 对应 Go: `errors.New("CronString does not have 5-7 fields")`
    #[error("cron string does not have 5-7 fields")]
    CronStringFieldCount,

    /// `@every` 格式的 Cron 字符串缺少间隔值。
    /// 对应 Go: `errors.New("CronString does not have interval after @every")`
    #[error("cron string does not have interval after @every")]
    CronStringMissingInterval,

    /// Range 解析/验证错误。包含字段名和详细说明。
    /// 对应 Go: `fmt.Errorf("%s has too many slashes/dashes", field)` 等
    #[error("{field}: {detail}")]
    RangeError {
        /// 出现错误的字段名（如 "Second", "Minute", "Hour"）。
        field: String,
        /// 错误的详细描述。
        detail: String,
    },

    /// 时区未找到。包含时区名称。
    /// 对应 Go: `time.LoadLocation` 返回的错误
    #[error("timezone not found: {0}")]
    TimezoneNotFound(String),

    /// 内部错误（不应在正常操作中出现）。
    #[error("{0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// RangeParseError — Range 解析错误
// ---------------------------------------------------------------------------

/// Range 字符串解析过程中可能出现的错误。
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RangeParseError {
    /// 值超出允许范围。
    /// 对应 Go: `errOutOfRange`
    #[error("out of range")]
    OutOfRange,

    /// 字符串不是合法的整数。
    /// 对应 Go: `strconv.Atoi` 的错误
    #[error("{0}")]
    ParseInt(#[from] std::num::ParseIntError),

    /// 时长格式无效。
    #[error("{0}")]
    ParseDuration(#[from] DurationParseError),
}

// ---------------------------------------------------------------------------
// DurationParseError — 时长解析错误
// ---------------------------------------------------------------------------

/// 时长字符串解析错误。
/// 对应 Go 代码中 `timestamp.ParseDuration` 的错误情况。
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum DurationParseError {
    /// 时长格式无效（如空字符串或无法识别的单位）。
    #[error("invalid duration format: {0}")]
    InvalidFormat(String),
}
