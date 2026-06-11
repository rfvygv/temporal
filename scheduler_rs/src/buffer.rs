//! 重叠策略（Overlap Policy）缓冲区处理。
//!
//! 移植自 Temporal 调度器的 `buffer.go`。
//!
//! # 设计意图
//!
//! 当调度器在短时间内产生多个工作流启动请求时，需要根据重叠策略决定：
//!
//! | 策略               | 行为                                               |
//! |-------------------|----------------------------------------------------|
//! | `ALLOW_ALL`       | 全部并发启动，不等待                                  |
//! | `SKIP`            | 跳过重叠的启动                                       |
//! | `BUFFER_ONE`      | 最多缓冲一个（队列深度=1）                             |
//! | `BUFFER_ALL`      | 缓冲所有重叠的启动（无限队列）                         |
//! | `CANCEL_OTHER`    | 取消正在运行的工作流，将当前项放入缓冲区               |
//! | `TERMINATE_OTHER` | 终止正在运行的工作流，将当前项放入缓冲区                |
//!
//! # 状态机流程
//!
//! ```text
//! for each start in buffer:
//!
//!   policy = resolve(start.overlap_policy)
//!
//!   if policy == ALLOW_ALL:
//!     → 添加到 concurrent_starts 列表（稍后全部并发启动）
//!
//!   if !is_running && non_overlapping_start 为空:
//!     → 直接启动此项（没有任何重叠，策略无关）
//!
//!   // 有重叠，应用策略:
//!   SKIP           → 丢弃
//!   BUFFER_ONE     → 缓冲区为空则放入，否则丢弃
//!   BUFFER_ALL     → 放入缓冲区
//!   CANCEL_OTHER   → 运行中: 标记取消 + 放入缓冲区
//!                    未运行: 替换 non_overlapping_start
//!   TERMINATE_OTHER → 同上（终止而非取消）
//! ```

use std::collections::BTreeMap;

use crate::types::ScheduleOverlapPolicy;

// ---------------------------------------------------------------------------
// Overlappable trait — 可重叠项特征
// ---------------------------------------------------------------------------

/// 可重叠项的抽象特征。
///
/// 任何放入缓冲区的条目都需要告知自己的重叠策略。
/// 对应 Go 代码中的 `Overlappable` 泛型接口。
pub trait Overlappable: Clone + PartialEq {
    /// 返回此项的重叠策略。
    fn overlap_policy(&self) -> ScheduleOverlapPolicy;
}

// ---------------------------------------------------------------------------
// ProcessBufferResult — 缓冲区处理结果
// ---------------------------------------------------------------------------

/// 执行重叠策略决策后的结果。
///
/// 对应 Go 代码中的 `ProcessBufferResult[T]` 泛型结构体。
#[derive(Debug, Clone)]
pub struct ProcessBufferResult<T> {
    /// 所有 "allow-all" 的启动项 — 这些可以同时并发启动。
    pub overlapping_starts: Vec<T>,

    /// 单个非重叠的启动项（如果有）。这是当前要启动的那一个。
    pub non_overlapping_start: Option<T>,

    /// 处理完成后剩余的缓冲区。下次 `process_buffer` 调用会重新处理。
    pub new_buffer: Vec<T>,

    /// 是否需要取消（请求取消）正在运行的工作流。
    /// 当 CANCEL_OTHER 策略项遇到运行中的工作流时设置。
    pub need_cancel: bool,

    /// 是否需要终止正在运行的工作流。
    /// 当 TERMINATE_OTHER 策略项遇到运行中的工作流时设置。
    pub need_terminate: bool,

    /// 因重叠而被跳过的启动项数量。
    pub overlap_skipped: i64,

    /// 按重叠策略分类的被跳过项数量统计。键为策略枚举值，值为跳过次数。
    pub overlap_skipped_by_policy: BTreeMap<ScheduleOverlapPolicy, i64>,
}

// ---------------------------------------------------------------------------
// process_buffer — 核心状态机
// ---------------------------------------------------------------------------

/// 处理待启动项的缓冲区，根据各自的重叠策略做出决策。
///
/// # 参数
///
/// * `buffer` — 待处理的缓冲启动条目。
/// * `is_running` — 当前是否有工作流正在运行。
/// * `resolve` — 将 `Unspecified` 解析为具体策略的函数
///   （通常使用调度器的当前默认重叠策略，如将 Unspecified 解析为 Skip）。
///
/// # 返回值
///
/// [`ProcessBufferResult`] 包含了哪些需要立即启动、哪些需要缓冲、
/// 是否需要取消/终止等决策。
pub fn process_buffer<T, F>(
    buffer: &[T],
    is_running: bool,
    resolve: F,
) -> ProcessBufferResult<T>
where
    T: Overlappable,
    F: Fn(ScheduleOverlapPolicy) -> ScheduleOverlapPolicy,
{
    let mut action = ProcessBufferResult {
        overlapping_starts: Vec::new(),
        non_overlapping_start: None,
        new_buffer: Vec::new(),
        need_cancel: false,
        need_terminate: false,
        overlap_skipped: 0,
        overlap_skipped_by_policy: BTreeMap::new(),
    };

    for start in buffer.iter() {
        // 第一步：解析重叠策略（将 Unspecified 映射为默认策略）
        let overlap_policy = resolve(start.overlap_policy());

        // 第二步：ALLOW_ALL 策略 — 收集到 concurrent 列表，所有此类项同时启动
        if overlap_policy == ScheduleOverlapPolicy::AllowAll {
            action.overlapping_starts.push(start.clone());
            continue;
        }

        // 第三步：无重叠情况 — 没有任何工作流在运行时，第一个非 ALLOW_ALL 项直接启动
        // 注意：即使策略是 SKIP，在没有重叠时也应该启动（SKIP 只在有重叠时生效）
        if !is_running && action.non_overlapping_start.is_none() {
            action.non_overlapping_start = Some(start.clone());
            continue;
        }

        // 第四步：有重叠 — 根据策略处理
        match overlap_policy {
            ScheduleOverlapPolicy::Skip => {
                // SKIP: 直接丢弃此项
                action.overlap_skipped += 1;
                *action
                    .overlap_skipped_by_policy
                    .entry(overlap_policy)
                    .or_insert(0) += 1;
            }
            ScheduleOverlapPolicy::BufferOne => {
                // BUFFER_ONE: 缓冲区中最多保留一个
                if action.new_buffer.is_empty() {
                    action.new_buffer.push(start.clone());
                } else {
                    action.overlap_skipped += 1;
                    *action
                        .overlap_skipped_by_policy
                        .entry(overlap_policy)
                        .or_insert(0) += 1;
                }
            }
            ScheduleOverlapPolicy::BufferAll => {
                // BUFFER_ALL: 无限制地放入缓冲区
                action.new_buffer.push(start.clone());
            }
            ScheduleOverlapPolicy::CancelOther => {
                // CANCEL_OTHER: 抢占式 — 取消当前运行的，用此项替换
                if is_running {
                    // 有实际运行的工作流 → 异步取消它，此项放入缓冲区等待取消完成
                    action.need_cancel = true;
                    action.new_buffer.push(start.clone());
                } else {
                    // 还没真正运行（只有 NonOverlappingStart 被设置了）→ 直接替换
                    action.non_overlapping_start = Some(start.clone());
                }
            }
            ScheduleOverlapPolicy::TerminateOther => {
                // TERMINATE_OTHER: 同 CANCEL_OTHER，但使用终止而非取消
                if is_running {
                    action.need_terminate = true;
                    action.new_buffer.push(start.clone());
                } else {
                    action.non_overlapping_start = Some(start.clone());
                }
            }
            ScheduleOverlapPolicy::Unspecified => {
                // 已在 resolve() 中处理，不应该到达此处
            }
            ScheduleOverlapPolicy::AllowAll => {
                // 已在上面处理，不应该到达此处
            }
        }
    }

    // 安全规则: 如果要取消/终止正在运行的工作流，那么同时启动任何允许全并发的
    // 工作流是毫无意义的 — 因为它们会立即被取消/终止。所以清空 concurrent 列表。
    if action.need_cancel || action.need_terminate {
        action.overlapping_starts.clear();
    }

    action
}

// ---------------------------------------------------------------------------
// 测试 — 覆盖所有重叠策略的组合场景
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 用于测试的简单启动项类型。
    #[derive(Debug, Clone, PartialEq)]
    struct TestStart {
        id: &'static str,
        policy: ScheduleOverlapPolicy,
    }

    impl Overlappable for TestStart {
        fn overlap_policy(&self) -> ScheduleOverlapPolicy {
            self.policy
        }
    }

    fn make_start(id: &'static str, policy: ScheduleOverlapPolicy) -> TestStart {
        TestStart { id, policy }
    }

    /// 将 Unspecified 解析为 SKIP（测试默认行为）。
    fn resolve(p: ScheduleOverlapPolicy) -> ScheduleOverlapPolicy {
        match p {
            ScheduleOverlapPolicy::Unspecified => ScheduleOverlapPolicy::Skip,
            other => other,
        }
    }

    /// 空缓冲区 → 无操作。
    #[test]
    fn test_buffer_empty() {
        let result = process_buffer::<TestStart, _>(&[], false, resolve);
        assert!(result.non_overlapping_start.is_none());
        assert!(result.overlapping_starts.is_empty());
        assert!(result.new_buffer.is_empty());
        assert!(!result.need_cancel);
        assert!(!result.need_terminate);
    }

    /// 无运行中工作流时，第一项（即使策略是 SKIP）直接启动。
    /// 原因：SKIP 策略只在有重叠（即有工作流运行）时生效。
    #[test]
    fn test_buffer_first_goes_when_not_running() {
        let buf = vec![make_start("a", ScheduleOverlapPolicy::Skip)];
        let result = process_buffer(&buf, false, resolve);
        assert_eq!(
            result.non_overlapping_start.as_ref().map(|s| s.id),
            Some("a")
        );
    }

    /// 有运行中工作流时，SKIP 项被丢弃并计数。
    #[test]
    fn test_buffer_skip_when_running() {
        let buf = vec![make_start("a", ScheduleOverlapPolicy::Skip)];
        let result = process_buffer(&buf, true, resolve);
        assert!(result.non_overlapping_start.is_none());
        assert_eq!(result.overlap_skipped, 1);
    }

    /// BUFFER_ONE: 队列深度=1。
    /// - 未运行: a 启动, b 缓冲
    /// - 运行中: a 缓冲, b 被跳过（缓冲区已满）
    #[test]
    fn test_buffer_one() {
        let buf = vec![
            make_start("a", ScheduleOverlapPolicy::BufferOne),
            make_start("b", ScheduleOverlapPolicy::BufferOne),
        ];
        // 未运行 → 第一项直接启动，第二项缓冲
        let result = process_buffer(&buf, false, resolve);
        assert_eq!(
            result.non_overlapping_start.as_ref().map(|s| s.id),
            Some("a")
        );
        assert_eq!(result.new_buffer.len(), 1);
        assert_eq!(result.new_buffer[0].id, "b");

        // 运行中 → 第一项进入缓冲，第二项被跳过
        let result2 = process_buffer(&buf, true, resolve);
        assert!(result2.non_overlapping_start.is_none());
        assert_eq!(result2.new_buffer.len(), 1);
        assert_eq!(result2.new_buffer[0].id, "a");
        assert_eq!(result2.overlap_skipped, 1);
    }

    /// BUFFER_ALL: 无限缓冲。
    /// - 未运行: 第一项启动，剩余缓冲
    /// - 运行中: 全部放入缓冲
    #[test]
    fn test_buffer_all() {
        let buf = vec![
            make_start("a", ScheduleOverlapPolicy::BufferAll),
            make_start("b", ScheduleOverlapPolicy::BufferAll),
        ];
        let result = process_buffer(&buf, false, resolve);
        assert_eq!(
            result.non_overlapping_start.as_ref().map(|s| s.id),
            Some("a")
        );
        assert_eq!(result.new_buffer.len(), 1);
        assert_eq!(result.new_buffer[0].id, "b");

        let result2 = process_buffer(&buf, true, resolve);
        assert!(result2.non_overlapping_start.is_none());
        assert_eq!(result2.new_buffer.len(), 2);
        assert_eq!(result2.new_buffer[0].id, "a");
        assert_eq!(result2.new_buffer[1].id, "b");
    }

    /// CANCEL_OTHER: 抢占式取消。
    /// - 运行中: need_cancel=true，此项进入缓冲（等待取消完成后执行）
    /// - 未运行: 直接替换 NonOverlappingStart（多个 CANCEL_OTHER 中最后一个胜出）
    #[test]
    fn test_buffer_cancel_other() {
        let buf = vec![make_start("a", ScheduleOverlapPolicy::CancelOther)];

        // 运行中 → 需要取消
        let result = process_buffer(&buf, true, resolve);
        assert!(result.need_cancel);
        assert_eq!(result.new_buffer.len(), 1);
        assert_eq!(result.new_buffer[0].id, "a");

        // 未运行 → 替换
        let result2 = process_buffer(&buf, false, resolve);
        assert!(!result2.need_cancel);
        assert_eq!(
            result2.non_overlapping_start.as_ref().map(|s| s.id),
            Some("a")
        );
    }

    /// TERMINATE_OTHER: 抢占式终止（比 CANCEL 更强）。
    #[test]
    fn test_buffer_terminate_other() {
        let buf = vec![make_start("a", ScheduleOverlapPolicy::TerminateOther)];

        // 运行中 → 需要终止
        let result = process_buffer(&buf, true, resolve);
        assert!(result.need_terminate);
        assert_eq!(result.new_buffer.len(), 1);

        // 未运行 → 替换
        let result2 = process_buffer(&buf, false, resolve);
        assert!(!result2.need_terminate);
        assert_eq!(
            result2.non_overlapping_start.as_ref().map(|s| s.id),
            Some("a")
        );
    }

    /// ALLOW_ALL 项全部收集到 concurrent 列表，非 ALLOW_ALL 项正常处理。
    #[test]
    fn test_buffer_allow_all() {
        let buf = vec![
            make_start("a", ScheduleOverlapPolicy::AllowAll),
            make_start("b", ScheduleOverlapPolicy::AllowAll),
            make_start("c", ScheduleOverlapPolicy::Skip),
        ];
        let result = process_buffer(&buf, false, resolve);
        assert_eq!(result.overlapping_starts.len(), 2);
        assert_eq!(result.overlapping_starts[0].id, "a");
        assert_eq!(result.overlapping_starts[1].id, "b");
        assert_eq!(
            result.non_overlapping_start.as_ref().map(|s| s.id),
            Some("c")
        );
    }

    /// 取消/终止时，ALLOW_ALL 项会被清空。
    /// 原因：它们会被立即取消/终止，启动毫无意义。
    #[test]
    fn test_cancel_clears_allow_all() {
        let buf = vec![
            make_start("a", ScheduleOverlapPolicy::AllowAll),
            make_start("b", ScheduleOverlapPolicy::CancelOther),
        ];
        let result = process_buffer(&buf, true, resolve);
        // 有 cancel/terminate 时 ALLOW_ALL 项被清空
        assert!(result.overlapping_starts.is_empty());
        assert!(result.need_cancel);
    }
}
