//! 调度规格的 Proto 等价类型。
//!
//! 这些结构体镜像 Go 代码中的 Protobuf 类型，来自：
//! - `go.temporal.io/api/schedule/v1` — 调度规格定义
//! - `go.temporal.io/api/enums/v1` — 枚举定义
//! - `google.protobuf.Duration` / `google.protobuf.Timestamp` — 标准类型
//!
//! # 类型层级关系
//!
//! ```text
//! ScheduleSpec (顶层调度规格)
//! ├── StructuredCalendarSpec[]  ← 编译后的结构化日历
//! │   └── Range[]              ← 每个字段的值域（秒/分/时/日/月/年/星期）
//! ├── IntervalSpec[]           ← 间隔规格（如"每90分钟"）
//! ├── CalendarSpec[]           ← 原始日历字符串（Crontab 格式）
//! └── ExcludeCalendarSpec[]    ← 排除日历
//! ```

use std::time::Duration;

use jiff::Timestamp;

// ---------------------------------------------------------------------------
// Range — 值域
// ---------------------------------------------------------------------------

/// 对应 Protobuf 消息 `schedulepb.Range`。
///
/// 表示一个字段的匹配范围，支持起始值、结束值和步长。
/// 编码约定（与 Proto 一致以减小序列化体积）：
/// - `end = 0` 表示"与 start 相同"（即单值匹配）
/// - `step = 0` 表示 step = 1
///
/// # 示例
///
/// | 表达式   | start | end | step | 语义              |
/// |---------|-------|-----|------|-------------------|
/// | `5`     | 5     | 0   | 0    | 精确匹配 5         |
/// | `1-5`   | 1     | 5   | 0    | 匹配 1,2,3,4,5     |
/// | `1-5/2` | 1     | 5   | 2    | 匹配 1,3,5         |
/// | `3/5`   | 3     | 0   | 5    | 匹配 3,8,13,18,... |
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    /// 起始值（包含）。
    pub start: i32,
    /// 结束值（包含）。0 表示与 start 相同。
    pub end: i32,
    /// 步长。0 表示步长为 1。
    pub step: i32,
}

impl Range {
    /// 创建一个单值 Range（end=0, step=0）。
    pub fn new(start: i32) -> Self {
        Self {
            start,
            end: 0,
            step: 0,
        }
    }

    /// 获取有效结束值。当 `end == 0` 时返回 `start`。
    /// 对应 Go 代码中 Range 的 GetEnd() 语义。
    pub fn effective_end(&self) -> i32 {
        if self.end == 0 {
            self.start
        } else {
            self.end
        }
    }

    /// 获取有效步长值。当 `step == 0` 时返回 1。
    /// 对应 Go 代码中 Range 的 GetStep() 语义。
    pub fn effective_step(&self) -> i32 {
        if self.step == 0 {
            1
        } else {
            self.step
        }
    }
}

// ---------------------------------------------------------------------------
// StructuredCalendarSpec — 结构化日历规格
// ---------------------------------------------------------------------------

/// 对应 Protobuf 消息 `schedulepb.StructuredCalendarSpec`。
///
/// 每个字段是一个 `Range` 列表。日历匹配时，所有字段必须同时匹配。
///
/// # 字段含义
///
/// | 字段            | 范围       | 说明                      |
/// |----------------|-----------|--------------------------|
/// | `second`       | 0-59      | 秒                        |
/// | `minute`       | 0-59      | 分                        |
/// | `hour`         | 0-23      | 时                        |
/// | `day_of_month` | 1-31      | 日                        |
/// | `month`        | 1-12      | 月（1=一月）               |
/// | `year`         | 2000-2100 | 年（空=全部）              |
/// | `day_of_week`  | 0-6       | 星期（0=周日, 7 映射为 0） |
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StructuredCalendarSpec {
    /// 秒 (0-59)。默认值: `[0]`。
    pub second: Vec<Range>,
    /// 分 (0-59)。默认值: `[0]`。
    pub minute: Vec<Range>,
    /// 时 (0-23)。默认值: `[0]`。
    pub hour: Vec<Range>,
    /// 日 (1-31)。默认值: `[1-31]`。
    pub day_of_month: Vec<Range>,
    /// 月 (1-12)。默认值: `[1-12]`。
    pub month: Vec<Range>,
    /// 年 (2000-2100)。默认值: `[]`（空列表=全部年份）。
    pub year: Vec<Range>,
    /// 星期 (0-6, 0=周日)。默认值: `[0-6]`。
    pub day_of_week: Vec<Range>,
    /// 可选注释（最多 200 字符）。
    pub comment: String,
}

// ---------------------------------------------------------------------------
// CalendarSpec — 字符串形式的日历规格（Crontab 格式）
// ---------------------------------------------------------------------------

/// 对应 Protobuf 消息 `schedulepb.CalendarSpec`。
///
/// 与 `StructuredCalendarSpec` 不同，每个字段是 Crontab 风格的字符串
/// （如 `"5,7"`, `"*"`, `"1-5/2"`），在编译时解析为结构化的 `Range` 列表。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CalendarSpec {
    /// 秒字段字符串。默认值: `"0"`。
    pub second: String,
    /// 分字段字符串。默认值: `"0"`。
    pub minute: String,
    /// 时字段字符串。默认值: `"0"`。
    pub hour: String,
    /// 日字段字符串。默认值: `"*"`。
    pub day_of_month: String,
    /// 月字段字符串。默认值: `"*"`。
    pub month: String,
    /// 星期字段字符串。默认值: `"*"`。
    pub day_of_week: String,
    /// 年字段字符串。默认值: `"*"`。
    pub year: String,
    /// 可选注释。
    pub comment: String,
}

// ---------------------------------------------------------------------------
// IntervalSpec — 间隔规格
// ---------------------------------------------------------------------------

/// 对应 Protobuf 消息 `schedulepb.IntervalSpec`。
///
/// 表示以固定间隔触发的调度。支持相位偏移。
///
/// # 示例
///
/// - `@every 90m` → `interval=90m, phase=0`
/// - `@every 90m/5m` → `interval=90m, phase=5m`（第5分钟开始，之后每90分钟）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalSpec {
    /// 间隔时长（最小 1 秒）。
    pub interval: Duration,
    /// 相位偏移（必须 < interval）。
    pub phase: Duration,
}

// ---------------------------------------------------------------------------
// ScheduleOverlapPolicy — 重叠策略枚举
// ---------------------------------------------------------------------------

/// 对应 Protobuf 枚举 `enumspb.ScheduleOverlapPolicy`。
///
/// 当一个调度触发时，如果上一次触发的工作流还在运行，
/// 此策略决定如何处理这个重叠。
///
/// # 策略语义
///
/// | 变体             | 编号 | 行为                                         |
/// |-----------------|-----|----------------------------------------------|
/// | `Unspecified`   | 0   | 未指定（自动解析为 Skip）                      |
/// | `Skip`          | 1   | 跳过当前触发                                  |
/// | `BufferOne`     | 2   | 缓冲一个（队列深度=1）                        |
/// | `BufferAll`     | 3   | 缓冲所有（无限队列）                           |
/// | `CancelOther`   | 4   | 取消正在运行的，用新触发替换                   |
/// | `TerminateOther`| 5   | 终止正在运行的，用新触发替换                   |
/// | `AllowAll`      | 6   | 全部并发启动（不等待）                         |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScheduleOverlapPolicy {
    /// 未指定 — 将被解析为调度器的默认策略（通常是 Skip）。
    Unspecified,
    /// 跳过: 如果有重叠则丢弃当前触发。
    Skip,
    /// 缓冲一个: 重叠时最多排队一个。
    BufferOne,
    /// 缓冲所有: 重叠时全部排队等待。
    BufferAll,
    /// 取消其他: 取消正在运行的，立即启动当前。
    CancelOther,
    /// 终止其他: 终止正在运行的，立即启动当前。
    TerminateOther,
    /// 允许全部: 全部并发执行，互不等待。
    AllowAll,
}

impl ScheduleOverlapPolicy {
    /// 将 `Unspecified` 解析为默认策略，非 Unspecified 值保持不变。
    pub fn resolve(self, default: Self) -> Self {
        match self {
            Self::Unspecified => default,
            other => other,
        }
    }
}

// ---------------------------------------------------------------------------
// ScheduleSpec — 顶层调度规格（Proto 等价）
// ---------------------------------------------------------------------------

/// 对应 Protobuf 消息 `schedulepb.ScheduleSpec`。
///
/// 这是用户定义的调度触发规则。一个 `ScheduleSpec` 可以同时包含多种
/// 触发方式（日历、间隔、Cron 字符串），触发时间取所有方式中最早的那个。
///
/// # 规范化
///
/// 用户可以使用 `CalendarSpec`（字符串格式）或 `CronString` 输入，
/// 在调用 [`crate::spec::canonicalize_spec`] 之后，它们会被转换为
/// `StructuredCalendarSpec` 和 `IntervalSpec` 的统一形式。
///
/// # 字段说明
///
/// | 字段                          | 说明                                    |
/// |------------------------------|-----------------------------------------|
/// | `structured_calendar`        | 编译后的结构化日历（规范化后填充）       |
/// | `interval`                   | 间隔规格列表                            |
/// | `cron_string`                | 原始 Cron 字符串（规范化后清空）         |
/// | `calendar`                   | 原始日历字符串（规范化后清空）           |
/// | `start_time` / `end_time`    | 调度有效时间范围 [start_time, end_time)  |
/// | `jitter`                     | 最大随机偏移量（分散负载）              |
/// | `timezone_name` / `timezone_data` | 时区名称或 TZif 二进制数据          |
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ScheduleSpec {
    /// 编译后的结构化日历（由 `calendar` 字段或 Cron 字符串解析而来）。
    pub structured_calendar: Vec<StructuredCalendarSpec>,
    /// 编译后的排除日历。
    pub exclude_structured_calendar: Vec<StructuredCalendarSpec>,
    /// 间隔规格列表。
    pub interval: Vec<IntervalSpec>,
    /// 原始 Cron 字符串（如 `"23 5,7 * * *"`）。
    pub cron_string: Vec<String>,
    /// 原始日历规格（字符串格式）。
    pub calendar: Vec<CalendarSpec>,
    /// 原始排除日历规格。
    pub exclude_calendar: Vec<CalendarSpec>,
    /// 包含的起始时间（调度不会在此时间之前触发）。
    pub start_time: Option<Timestamp>,
    /// 排他的结束时间（调度不会在此时间之后触发）。
    pub end_time: Option<Timestamp>,
    /// 最大 Jitter 时长（为 0 表示无 jitter）。
    pub jitter: Option<Duration>,
    /// IANA 时区名称（如 `"Asia/Shanghai"`）。
    pub timezone_name: String,
    /// 时区的 TZif 二进制数据。存在时优先于 `timezone_name`。
    pub timezone_data: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// GetNextTimeResult — GetNextTime 的返回结果
// ---------------------------------------------------------------------------

/// [`CompiledSpec::get_next_time`] 的返回结果。
///
/// 包含两个时间戳：
/// - `nominal`: 调度匹配的原始时间（jitter 之前）
/// - `next`: 添加确定性 jitter 后的实际启动时间
///
/// 当没有更多匹配时间时（调度已耗尽或超过 EndTime），两者均为 `None`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetNextTimeResult {
    /// Jitter 之前的调度匹配时间。
    pub nominal: Option<Timestamp>,
    /// Jitter 之后的实际启动时间。
    pub next: Option<Timestamp>,
}

impl Default for GetNextTimeResult {
    fn default() -> Self {
        Self {
            nominal: None,
            next: None,
        }
    }
}

// ---------------------------------------------------------------------------
// BufferedStart — 缓冲启动条目
// ---------------------------------------------------------------------------

/// 缓冲的启动条目，供重叠策略处理使用。
///
/// 对应 Go 代码中的 `schedulespb.BufferedStart`。
///
/// 当调度器计算出多个启动时间后，它们被缓冲在这里，然后由
/// [`crate::buffer::process_buffer`] 根据重叠策略决定执行顺序。
#[derive(Debug, Clone)]
pub struct BufferedStart {
    /// 调度的原始名义时间。
    pub nominal_time: Timestamp,
    /// 实际的计划启动时间（nominal + jitter）。
    pub actual_time: Timestamp,
    /// 此项的重叠策略。
    pub overlap_policy: ScheduleOverlapPolicy,
    /// 是否手动触发（如 TriggerImmediately 或 Backfill）。
    pub manual: bool,
}

impl BufferedStart {
    /// 获取重叠策略，实现 `Overlappable` trait。
    pub fn get_overlap_policy(&self) -> ScheduleOverlapPolicy {
        self.overlap_policy
    }
}

// ---------------------------------------------------------------------------
// 调度器常量
// ---------------------------------------------------------------------------

/// 日历规格中允许的最小年份。
/// 对应 Go: `minCalendarYear = 2000`
pub const MIN_CALENDAR_YEAR: i32 = 2000;

/// 日历规格中允许的最大年份。
/// 超过此年份的匹配时间将不会返回。
/// 对应 Go: `maxCalendarYear = 2100`
pub const MAX_CALENDAR_YEAR: i32 = 2100;

/// 日历注释字段的最大字符数。
/// 对应 Go: `maxCommentLen = 200`
pub const MAX_COMMENT_LEN: usize = 200;
