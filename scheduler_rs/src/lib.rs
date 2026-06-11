//! Temporal 调度器规格引擎 — Rust 重写。
//!
//! 本 crate 将 Go 版本 Temporal 服务端 `service/worker/scheduler` 包中的
//! 调度规格编译、下次时间计算、日历解析、Cron 字符串解析和重叠策略缓冲区
//! 逻辑移植到 Rust。
//!
//! # 模块结构
//!
//! | 模块          | Go 源文件          | 功能                                              |
//! |--------------|-------------------|---------------------------------------------------|
//! | [`types`]    | proto 生成类型     | 调度规格数据结构（手写 Rust 等价类型）               |
//! | [`calendar`] | `calendar.go`     | 日历/Cron 解析、编译型匹配器、next() 搜索算法       |
//! | [`spec`]     | `spec.go`         | `CompiledSpec` 规格编译、`SpecBuilder` 时区缓存、GetNextTime |
//! | [`buffer`]   | `buffer.go`       | 重叠策略缓冲区处理 (SKIP/BUFFER/CANCEL/TERMINATE)  |
//! | [`error`]    | —                 | 错误类型定义                                        |
//!
//! # 快速开始
//!
//! ```rust
//! use temporal_scheduler::{
//!     types::{ScheduleSpec, IntervalSpec},
//!     spec::SpecBuilder,
//! };
//! use std::time::Duration;
//!
//! // 创建 SpecBuilder（内置时区缓存，TTL=24h，容量 1000）
//! let builder = SpecBuilder::new();
//!
//! // 定义调度：每 1 小时触发一次
//! let spec = ScheduleSpec {
//!     interval: vec![IntervalSpec {
//!         interval: Duration::from_secs(3600),
//!         phase: Duration::ZERO,
//!     }],
//!     ..Default::default()
//! };
//!
//! // 编译为 CompiledSpec
//! let compiled = builder.new_compiled_spec(&spec).unwrap();
//!
//! // 获取参考时间之后的下一个调度时间
//! use jiff::Timestamp;
//! let after = Timestamp::from_second(1_700_000_000).unwrap();
//! let result = compiled.get_next_time("my-schedule-id", after);
//! // result.nominal → jitter 前的名义时间
//! // result.next    → jitter 后的实际启动时间
//! println!("下次触发: {:?}", result.next);
//! ```
//!
//! # 核心设计决策
//!
//! | 决策           | Go 版本         | Rust 版本                          | 理由                              |
//! |---------------|-----------------|-----------------------------------|-----------------------------------|
//! | 时间库         | `time.Time`     | `jiff::Timestamp`                 | DST 正确处理、TZif 加载、无已知 Bug |
//! | 哈希算法       | `go-farm`       | `farmhash` crate                  | 相同算法，保证确定性 Jitter 一致    |
//! | 缓存           | `cache.Cache`   | `moka::sync::Cache`               | TTL + 容量限制，线程安全高性能     |
//! | 泛型           | Go 泛型         | Rust 泛型 + trait                 | 零成本抽象                         |
//! | 年份类型       | `int`           | `i16` (jiff) / `i32` (内部)       | jiff 的 Date::new(year: i16) 约束 |
//! | 错误处理       | error 哨兵值    | `thiserror::Error` 枚举           | 类型安全，模式匹配                 |

pub mod buffer;
pub mod calendar;
pub mod error;
pub mod spec;
pub mod types;

// 重新导出常用类型，方便外部使用。
pub use buffer::{process_buffer, Overlappable, ProcessBufferResult};
pub use calendar::{
    make_range, parse_calendar_to_structured, parse_cron_string, parse_duration, validate_interval,
    validate_structured_calendar, CompiledCalendar, ParseMode,
};
pub use error::SpecError;
pub use spec::{
    add_jitter, canonicalize_spec, clean_spec, next_interval_time, CompiledSpec, SpecBuilder,
};
pub use types::*;
