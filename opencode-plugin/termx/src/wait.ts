// 共享等待策略：exec 默认同步等待与 read busy 等待使用同一组 timeout/poll。
//
// 超时只停止等待，不改变终端/进程，也不中断正在运行的命令。

/**
 * 等待 busy 变为 false；超时只停止等待，不改变终端/进程。
 *
 * 每次轮询重新读取状态（异步谓词），避免用发送前的快照误判完成。
 *
 * @param isBusy 当前是否 busy（异步读取 transcript/状态）
 * @param timeoutMs 最长等待（毫秒）；0 = 不等待
 * @param pollMs 轮询间隔（毫秒），最小 1
 * @returns Promise 在 idle 或超时后 resolve
 */
export async function waitForIdle(
  isBusy: () => Promise<boolean>,
  timeoutMs: number,
  pollMs: number,
): Promise<void> {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  while (Date.now() < deadline) {
    if (!(await isBusy())) return;
    const remaining = deadline - Date.now();
    if (remaining <= 0) return;
    const delay = Math.min(Math.max(1, pollMs), remaining);
    await new Promise<void>((resolve) => setTimeout(resolve, delay));
  }
}
