# Temporal 调度器规格引擎 — Rust 重构设计文档

> 移植自 Go 版 Temporal 服务端 `service/worker/scheduler/` 包
>
> 版本: 0.1.0 | 日期: 2026-06-11

---

## 一、模块总览

本 crate 将 Temporal 调度器（Scheduler V1 / Legacy）中负责**计算下次工作流启动时间**的核心逻辑用 Rust 重写。
涉及 Go 源码中三个文件：

```
service/worker/scheduler/
├── spec.go      (375 行) → scheduler_rs/src/spec.rs      (520 行)
├── calendar.go  (561 行) → scheduler_rs/src/calendar.rs  (950 行)
└── buffer.go    (114 行) → scheduler_rs/src/buffer.rs    (310 行)
```

### 目录结构

```
scheduler_rs/
├── Cargo.toml           # 依赖: jiff, farmhash, moka, thiserror, serde
├── DESIGN.md            # 本文档
├── src/
│   ├── lib.rs           # 库根 + 重新导出
│   ├── types.rs         # Proto 等价类型（手写）
│   ├── error.rs         # 错误类型枚举
│   ├── calendar.rs      # 日历编译/匹配/搜索/Cron解析
│   ├── spec.rs          # CompiledSpec, SpecBuilder, GetNextTime
│   └── buffer.rs        # 重叠策略缓冲区状态机
└── tests/               # 集成测试（可扩展）
```

---

## 二、数据流与架构

### 2.1 整体数据流

```
用户输入 (ScheduleSpec)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ SpecBuilder::new_compiled_spec()                     │
│                                                      │
│  1. canonicalize_spec()                              │
│     ├── CalendarSpec (字符串) → StructuredCalendar   │
│     ├── CronString           → StructuredCalendar    │
│     │                        → IntervalSpec          │
│     └── 时区一致性检查                                │
│                                                      │
│  2. load_timezone()                                  │
│     ├── timezone_data 存在? → TZif 二进制加载         │
│     └── timezone_name → IANA 名称 (缓存 24h)         │
│                                                      │
│  3. 编译结构化日历 → Vec<CompiledCalendar>            │
│     每个 StructuredCalendar → BitMatcher × 7 + YearMatcher │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
              CompiledSpec
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│ CompiledSpec::get_next_time(jitter_seed, after)      │
│                                                      │
│  loop:                                               │
│    raw_next_time(after)                              │
│      ├── calendar[i].next(after)  ─┐                │
│      └── interval[i].next(after)  ─┤ 取最早          │
│                                    │                │
│    check: > EndTime? > year 2100? ─┤ 返回 None      │
│    check: is_excluded?            ─┤ continue       │
│    else break                                          │
│                                                      │
│  add_jitter(seed, nominal, maxJitter)                │
│    → FarmHash Fingerprint32 → 确定性 jitter          │
│                                                      │
│  return { nominal, next }                            │
└─────────────────────────────────────────────────────┘
```

### 2.2 模块依赖关系

```
            ┌─────────┐
            │  types  │  ← 所有模块的基础类型
            └────┬────┘
                 │
      ┌──────────┼──────────┐
      │          │          │
  ┌───┴───┐ ┌───┴───┐ ┌───┴───┐
  │ error │ │calendar│ │buffer │
  └───────┘ └───┬───┘ └───────┘
                 │
            ┌───┴───┐
            │ spec  │  ← 依赖 calendar, types, error
            └───┬───┘
                 │
            ┌───┴───┐
            │  lib  │  ← 重新导出所有公共 API
            └───────┘
```

---

## 三、核心数据结构详解

### 3.1 `SpecBuilder` — 编译入口

```
SpecBuilder
├── tz_cache: MokaCache<String, Result<TimeZone, String>>
│   ├── 容量: 1000
│   ├── TTL: 24 小时
│   └── 缓存负结果（不存在的时区名）
│
├── new_compiled_spec(spec) → Result<CompiledSpec, SpecError>
│   1. canonicalize_spec(spec)?  // 规范化所有输入格式
│   2. load_timezone(&spec)?     // 加载/缓存时区
│   3. 编译 calendar + excludes  // 构建 CompiledCalendar 列表
```

### 3.2 `CompiledSpec` — 编译后的规格

```
CompiledSpec
├── spec: ScheduleSpec     // 规范化后的规格（保留原始数据）
├── tz: TimeZone           // 解析后的时区
├── calendar: Vec<CompiledCalendar>   // 正向匹配日历
└── excludes: Vec<CompiledCalendar>   // 排除日历
```

### 3.3 `CompiledCalendar` — 编译后的日历

```
CompiledCalendar
├── tz: TimeZone
├── year: YearMatcher             ← 年匹配器
│   ├── All             → 任何年份都匹配（空 Range 列表）
│   └── Values(Vec)     → 稀疏值列表（如 [2024, 2025]）
├── month: BitMatcher            ← 月 (1-12)
├── day_of_month: BitMatcher     ← 日 (1-31)
├── day_of_week: BitMatcher      ← 星期 (0=Sun..6=Sat)
├── hour: BitMatcher             ← 时 (0-23)
├── minute: BitMatcher           ← 分 (0-59)
└── second: BitMatcher           ← 秒 (0-59)
```

**BitMatcher 原理**:
```
u64 bitset:
  bit 0 → 值 0 匹配?
  bit 1 → 值 1 匹配?
  ...
  bit 63 → 值 63 匹配?

示例: 匹配小时 {1,3,4,5}
  bits = (1<<1)|(1<<3)|(1<<4)|(1<<5) = 0b...00111010

O(1) 匹配: (1 << v) & bits != 0
```

### 3.4 `ScheduleSpec` — 用户规格

```
ScheduleSpec
├── structured_calendar: Vec<StructuredCalendarSpec>  // 编译后填充
├── exclude_structured_calendar: Vec<StructuredCalendarSpec>
├── interval: Vec<IntervalSpec>         // 如: 每90分钟
├── cron_string: Vec<String>           // 如: "23 5,7 * * *"
├── calendar: Vec<CalendarSpec>        // 原始字符串字段（编译后清空）
├── exclude_calendar: Vec<CalendarSpec>
├── start_time: Option<Timestamp>      // 有效开始时间（含）
├── end_time: Option<Timestamp>        // 有效结束时间（不含）
├── jitter: Option<Duration>           // 最大随机偏移
├── timezone_name: String              // IANA 时区名
└── timezone_data: Option<Vec<u8>>     // TZif 二进制数据（优先）
```

### 3.5 `StructuredCalendarSpec` — 结构化日历

```
StructuredCalendarSpec
├── second: Vec<Range>       // 默认 [0]
├── minute: Vec<Range>       // 默认 [0]
├── hour: Vec<Range>         // 默认 [0]
├── day_of_month: Vec<Range> // 默认 [1-31]
├── month: Vec<Range>        // 默认 [1-12]
├── year: Vec<Range>         // 默认 [] (空=全部)
├── day_of_week: Vec<Range>  // 默认 [0-6]
└── comment: String          // 最多200字符
```

### 3.6 `Range` — 值域

```
Range
├── start: i32   // 起始值
├── end: i32     // 0=与start相同
└── step: i32    // 0=步长1
```

---

## 四、核心算法详解

### 4.1 `get_next_time()` — 找到下一个匹配时间

这是整个调度引擎的核心方法。

**算法伪代码**:

```
function get_next_time(jitter_seed, after):
    // 1. 处理边界: 如果 after 在 start_time 之前，前移到 start_time-1s
    if after < start_time:
        after = start_time - 1s  // 减1s，使得 start_time 本身也可匹配

    // 2. 循环查找未被排除的 nominal 时间
    nominal = loop:
        next = raw_next_time(after)
        if next is None:
            return None  // 没有更多匹配时间

        if next > end_time or next.year > 2100:
            return None  // 超出有效范围

        if not is_excluded(next):
            nominal = next; break  // 找到有效 nominal

        after = next  // 被排除，继续搜索

    // 3. 计算 jitter 上限
    max_jitter = spec.jitter or 0
    following = raw_next_time(nominal)
    if following exists:
        max_jitter = min(max_jitter, following - nominal)

    // 4. 添加确定性 jitter
    next = add_jitter(jitter_seed, nominal, max_jitter)

    return { nominal, next }
```

**时间复杂度**: 每次调用 O(C × I)，其中 C = 日历数量，I = 间隔数量。如果在排除列表中，可能需要多次迭代。

### 4.2 `raw_next_time()` — 日历 + 间隔的最早时间

```
function raw_next_time(after):
    min_ts = None

    // 所有日历中取最早
    for each calendar:
        t = calendar.next(after)
        min_ts = min(min_ts, t)

    // 所有间隔中取最早
    for each interval:
        next_secs = next_interval_time(interval, after.as_second())
        min_ts = min(min_ts, next_secs)

    return min_ts
```

### 4.3 `CompiledCalendar::next()` — 日历搜索

这是最复杂的单文件算法（~200 行），手动实现进位迭代：

```
function next(after):
    zoned = after.in(tz)
    y, mo, d, h, m, s = zoned.year, month, day, hour, minute, second

    s++  // 从下一秒开始
    检测 DST 回退重复小时 → dst_offset

    loop:
        // 归一化进位
        if s >= 60: m++; s = 0
        if m >= 60: h++; m = 0
            // 检测 DST 回退
            if 前一个小时和后一个小时的时间差 > 1h:
                回退 h, dst_offset = 1h
        if h >= 24: d++; h = 0
        if d > days_in_month(mo, y): mo++; d = 1
        if mo > 12: y++; mo = 1
        if y > 2100: return None

        // 从外到内匹配 (短路)
        if not year.matches(y):
            y++; mo=1; d=1; h=m=s=0; continue
        if not month.matches(mo):
            mo++; d=1; h=m=s=0; continue
        if not day.matches(d) or not weekday.matches:
            d++; h=m=s=0; continue
        if not hour.matches(h):
            h++; m=s=0; continue
        if not minute.matches(m):
            m++; s=0; continue
        if not second.matches(s):
            s++; continue

        // 全部匹配 — 构建时间
        civil_dt = Date(y, mo, d, h, m, s)
        zoned = tz.to_zoned(civil_dt)

        // DST 间隙检查
        if zoned.hour != h:
            h++; m=s=0; continue  // 跳过不存在的时间

        return zoned.timestamp + dst_offset
```

**复杂度**: O(years_to_search × 1秒步长)，但通常只需几步即可找到匹配。

**DST 处理**:
- **Spring-forward (跳跃)**: 某些本地时间不存在 → `tz.to_zoned()` 可能失败或产生不同的小时 → 跳过
- **Fall-back (回退)**: 同一本地时间出现两次 → 检测并标记 `dst_offset=1h`，确保取第二次出现

### 4.4 `next_interval_time()` — 间隔公式

```
interval_s = max(1, interval.seconds)
phase_s    = max(0, phase.seconds)
next       = ((ts - phase_s) / interval_s + 1) * interval_s + phase_s
```

**示例**: `@every 90m, phase=5m44s`, `after=12:53:02`
```
interval_s = 5400
phase_s    = 344
after_secs = (12*3600 + 53*60 + 2) = 46382
next       = ((46382 - 344) / 5400 + 1) * 5400 + 344
           = (46038 / 5400 + 1) * 5400 + 344
           = (8 + 1) * 5400 + 344
           = 48944
→ 13:35:44
```

### 4.5 `add_jitter()` — 确定性哈希 Jitter

```
bytes = nominal.to_rfc3339() + jitter_seed
hash  = FarmHash.Fingerprint32(bytes)
jitter_ms = (hash * max_jitter_ms) >> 32
next = nominal + jitter_ms
```

**关键属性**: 
- 相同 `(nominal, seed)` → 相同 jitter → **确定性**
- 不同 seed → 不同 jitter → 分散负载
- jitter ∈ [0, max_jitter) → 不会超过最大偏移
- 上限约束: jitter 不超过到下一个 nominal 的间隔

### 4.6 `process_buffer()` — 重叠策略状态机

```
for each start in buffer:
    policy = resolve(start.overlap_policy)
    
    if policy == ALLOW_ALL:
        overlapping_starts.push(start)
    elif !is_running and non_overlapping_start is None:
        non_overlapping_start = start
    else:
        match policy:
            SKIP           → skip++
            BUFFER_ONE     → buffer empty? push : skip++
            BUFFER_ALL     → push to buffer
            CANCEL_OTHER   → running? need_cancel=true + push : replace
            TERMINATE_OTHER → running? need_terminate=true + push : replace
    
    if need_cancel or need_terminate:
        overlapping_starts.clear()  // 安全规则
```

**策略表格**:

| 策略              | is_running=true | is_running=false（首个） | is_running=false（后续） |
|------------------|-----------------|------------------------|------------------------|
| ALLOW_ALL        | concurrent[]    | concurrent[]           | concurrent[]           |
| SKIP             | 丢弃            | 立即启动                | 丢弃                    |
| BUFFER_ONE       | buffer[0]       | 立即启动                | buffer[0] (满则丢弃)    |
| BUFFER_ALL       | buffer.append   | 立即启动                | buffer.append          |
| CANCEL_OTHER     | cancel + buffer | 立即启动                | 替换 NonOverlapping     |
| TERMINATE_OTHER  | terminate+buffer| 立即启动                | 替换 NonOverlapping     |

---

## 五、关键技术决策

### 5.1 Rust vs Go 对照表

| 方面           | Go 版本                      | Rust 版本                          | 选择原因                           |
|---------------|-----------------------------|-----------------------------------|------------------------------------|
| 时间库         | `time.Time` / `time.Location` | `jiff::Timestamp` / `jiff::tz::TimeZone` | DST 正确处理、TZif 加载、API 更安全 |
| 哈希           | `dgryski/go-farm`           | `farmhash` crate                  | 相同算法，Fingerprint32 一致        |
| 缓存           | `common/cache.Cache`        | `moka::sync::Cache`               | TTL+容量、线程安全、高性能          |
| 泛型           | Go 泛型 `[T Overlappable]`   | Rust 泛型 + `Overlappable` trait  | 零成本抽象                          |
| 年份类型       | `int`                        | `i16` (jiff边界) / `i32` (内部)    | jiff 的 Date::new(year: i16)       |
| 错误处理       | sentinel errors             | `thiserror::Error` 枚举            | 类型安全、模式匹配                  |
| Protobuf       | `google.golang.org/protobuf`| 手写等价结构体 + `serde`           | 避免 proto 编译依赖                 |

### 5.2 jiff 时间库的特殊处理

jiff 0.2 有几个与 Go `time.Time` 不同的 API 设计：

| 操作              | Go                           | Rust (jiff)                                    |
|------------------|------------------------------|------------------------------------------------|
| 创建时间           | `time.Date(y, mo, d, ...)`   | `civil::Date::new(y: i16, m: i8, d: i8)`       |
| 获取年份           | `t.Year() → int`             | `t.to_zoned(tz).datetime().year() → i16`        |
| 时间相减           | `t1.Sub(t2) → Duration`      | `t1.as_second() - t2.as_second() → i64` (推荐)  |
| 时间加法           | `t.Add(d) → Time`            | `t.checked_add(d) → Result<Timestamp, Error>`   |
| 时间减法           | `t.Add(-d) → Time`            | `t.checked_sub(d) → Result<Timestamp, Error>`   |
| DST 转换          | 自动                          | `tz.to_zoned(civil_dt) → Result<Zoned, Error>`  |

### 5.3 精度与上限

- **分辨率**: 1 秒（调度器不需要亚秒精度）
- **年份范围**: 2000-2100（对应 Go 的 `minCalendarYear`/`maxCalendarYear`）
- **Jitter 上限**: 使用 `u32::MAX` 毫秒（约 49.7 天）作为理论最大 jitter
- **时区缓存**: 容量 1000，TTL 24 小时

---

## 六、Cron 字符串解析

### 6.1 支持格式

```
标准 Cron:
  5字段:  min hour dom mon dow
  6字段:  min hour dom mon dow year
  7字段:  sec min hour dom mon dow year

特殊别名:
  @hourly    → "0 * * * *"
  @daily     → "0 0 * * *"
  @weekly    → "0 0 * * 0"
  @monthly   → "0 0 1 * *"
  @yearly    → "0 0 1 1 *"

间隔:
  @every 90m         → 每 90 分钟
  @every 3h/30m      → 每 3 小时, 相位偏移 30 分钟

时区前缀:
  CRON_TZ=Asia/Shanghai ...  或  TZ=Asia/Shanghai ...
```

### 6.2 Range 字符串语法

```
*          → 匹配所有 (min-max)
x          → 精确匹配 x
x-z        → 范围 [x, z] 包含
x-z/y      → 范围 [x, z], 步长 y
x/y        → [x, max], 步长 y
a,b,c      → 逗号分隔的多个值/范围
```

**周日特殊处理**: `7`（及范围中包含7的）自动映射到 `0`（Sunday）。当 step > 1 或范围为 `[start>1, 7]` 时，拆分为 `[0] + [start, 6]` 两个 Range。

---

## 七、测试覆盖

### 7.1 测试矩阵 (28 个测试)

| 模块     | 测试数 | 覆盖内容                                        |
|---------|--------|------------------------------------------------|
| buffer  | 8      | empty, first_goes, skip, buffer_one, buffer_all, cancel_other, terminate_other, cancel_clears_allow_all |
| calendar| 5      | days_in_month, predefined_cron, parse_duration, make_range_basic/default, bit_matcher |
| spec    | 14     | next_interval_time × 2, jitter × 2, interval_basic, calendar_basic, calendar_multiple, mixed_cal_interval, exclude, exclude_all, start_time, end_time, bounded_jitter, canonicalize_full |
| lib     | 1      | doc-test (快速开始示例)                          |

### 7.2 运行测试

```bash
cd scheduler_rs
cargo test
# 输出: test result: ok. 27 passed; 0 failed
```

---

## 八、使用示例

### 8.1 间隔调度

```rust
use temporal_scheduler::{types::*, spec::SpecBuilder};
use std::time::Duration;

let builder = SpecBuilder::new();
let spec = ScheduleSpec {
    interval: vec![IntervalSpec {
        interval: Duration::from_secs(90 * 60),
        phase: Duration::from_secs(5 * 60 + 44),
    }],
    ..Default::default()
};

let compiled = builder.new_compiled_spec(&spec).unwrap();

use jiff::Timestamp;
let after = Timestamp::from_second(1_700_000_000).unwrap();
let result = compiled.get_next_time("schedule-id", after);
// result.nominal → 名义时间
// result.next    → 带 jitter 的实际启动时间
```

### 8.2 日历调度

```rust
let spec = ScheduleSpec {
    calendar: vec![CalendarSpec {
        hour: "5,7".into(),
        minute: "23".into(),
        ..Default::default()
    }],
    ..Default::default()
};

let compiled = builder.new_compiled_spec(&spec).unwrap();
// 每天 5:23 和 7:23 各触发一次
```

### 8.3 排除时间

```rust
let spec = ScheduleSpec {
    interval: vec![IntervalSpec {
        interval: Duration::from_secs(3600),
        phase: Duration::ZERO,
    }],
    exclude_calendar: vec![CalendarSpec {
        hour: "12-14".into(),  // 排除 12:00-14:59
        minute: "*".into(),
        second: "*".into(),
        ..Default::default()
    }],
    ..Default::default()
};
// 每小时触发，但午饭时间除外
```

### 8.4 重叠策略

```rust
use temporal_scheduler::buffer::{process_buffer, Overlappable};

// 实现 Overlappable trait
impl Overlappable for BufferedStart {
    fn overlap_policy(&self) -> ScheduleOverlapPolicy {
        self.overlap_policy
    }
}

let result = process_buffer(&buffered_starts, is_running, |p| p.resolve(ScheduleOverlapPolicy::Skip));
// result.non_overlapping_start → 立即启动
// result.new_buffer            → 等待队列
// result.need_cancel           → 需要取消运行中的 WF
```

---

## 九、扩展计划

- [ ] 完整的功能测试套件（与 Go 版本的 `TestReplays` 对齐）
- [ ] 性能基准测试（对比 Go 版本）
- [ ] 自定义时区数据的更完整支持（`timezone_data` 字段）
- [ ] 与 Temporal Rust SDK 集成
- [ ] WASM 编译目标支持（浏览器端 Cron 解析）

---

## 十、相关文件索引

| 文件 | 路径 | 说明 |
|------|------|------|
| Go 源码 | `service/worker/scheduler/spec.go` | 原始 Go 实现 |
| Go 源码 | `service/worker/scheduler/calendar.go` | 日历/Cron 解析 |
| Go 源码 | `service/worker/scheduler/buffer.go` | 重叠策略处理 |
| Go 测试 | `service/worker/scheduler/spec_test.go` | 集成测试 |
| Rust 类型 | `src/types.rs` | 所有数据结构 |
| Rust 错误 | `src/error.rs` | 错误类型 |
| Rust 日历 | `src/calendar.rs` | 编译、匹配、解析 |
| Rust 规格 | `src/spec.rs` | GetNextTime、规范化 |
| Rust 缓冲 | `src/buffer.rs` | 重叠策略状态机 |
