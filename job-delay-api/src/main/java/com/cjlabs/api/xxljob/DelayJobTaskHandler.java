package com.cjlabs.api.xxljob;

import com.cjlabs.api.business.enums.RetryStrategyEnum;
import com.cjlabs.api.business.enums.TaskTypeEnum;
import com.cjlabs.api.business.mysql.DelayJobTask;
import com.cjlabs.api.business.service.DelayJobTaskService;
import com.cjlabs.boot.job.xxljob.AbstractXxlJobHandler;
import com.cjlabs.boot.job.xxljob.JobExecutionContext;
import com.cjlabs.core.time.FmkInstantUtil;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * 延迟任务处理 Handler - 支持分片执行
 * <p>
 * 分片处理流程：
 * 1. 获取分片信息（shardIndex, shardTotal）
 * 2. 动态获取最大 ID
 * 3. 计算分片的 ID 范围
 * 4. 分页查询范围内的待执行任务
 * 5. 对每个任务：原子更新 → 执行 → 更新结果
 * 6. 支持重试机制
 * <p>
 * 特点：
 * - 完全走索引，性能最优
 * - 数据增长自动适应
 * - 扩容无需改代码
 */
@Slf4j
@Component(value = "delayJobTaskHandler")
public class DelayJobTaskHandler extends AbstractXxlJobHandler {
    @Autowired
    private DelayJobTaskService delayJobTaskService;

    /**
     * 每次定时任务处理的最大任务数
     * <p>
     * "极限少量多次"策略：每次处理 100 条任务
     * <p>
     * 配置说明：
     * - 调度间隔：1 秒（在 XXL-Job Admin
     * - 单次处理：10 条任务
     * - 3 个分片，每个分片：每秒处理 10 条
     * - 每分钟吞吐：10 × 60 × 3 = 18,00 条
     * - 每小时吞吐：10 × 3600 × 3 = 1,08,000 条
     * - 每天吞吐：10 × 86,400 × 3 ≈ 2,592 万条
     * <p>
     * 优点：
     * ✅ 超短响应时间（1 秒内处理）
     * ✅ 内存占用极小（单次最多查询 100 条）
     * ✅ CPU 占用极低（快速处理后立即释放）
     * ✅ 数据库压力最小（频繁小查询）
     * ✅ 系统负载均衡（不会出现高峰）
     * ✅ 任务分布最均匀
     */
    private static final int BATCH_SIZE = 10;

    @Override
    protected int doExecute(JobExecutionContext context) throws Exception {
        int shardIndex = context.getShardIndex();
        int shardTotal = context.getShardTotal();

        log.info("[分片 {}] 开始处理本批次任务，批次大小: {}", context.getShardIndex(), BATCH_SIZE);

        // 查询待执行任务（只查询一批）
        List<DelayJobTask> tasks = delayJobTaskService.queryPendingTasksByShardAndPage(
                shardIndex, shardTotal, 1, BATCH_SIZE
        );

        if (tasks == null || tasks.isEmpty()) {
            log.info("[分片 {}] 本批次无待执行任务", context.getShardIndex());
            return 0;
        }

        log.info("[分片 {}] 获取到 {} 条待执行任务", context.getShardIndex(), tasks.size());

        // 处理任务
        int totalProcessed = 0;
        for (DelayJobTask task : tasks) {
            try {
                processOneTask(task, context);
                totalProcessed++;
            } catch (Exception e) {
                log.error("[分片 {}] 处理任务失败，taskId: {}", context.getShardIndex(), task.getId(), e);
            }
        }

        log.info("[分片 {}] 本批次处理完成，共处理 {} 条", context.getShardIndex(), totalProcessed);

        return totalProcessed;
    }

    /**
     * 处理单个任务
     * <p>
     * 三步法：
     * 1. 原子性更新状态为 PROCESSING（防重复）
     * 2. 执行业务逻辑
     * 3. 更新最终状态（成功或重试）
     */
    private void processOneTask(DelayJobTask task, JobExecutionContext context) throws Exception {
        long taskId = task.getId();

        // 步骤 1：原子性更新状态为 PROCESSING
        boolean updated = delayJobTaskService.updateTaskStatusToProcessing(taskId);

        if (!updated) {
            log.debug("任务 {} 已被其他实例处理，跳过", taskId);
            return;
        }

        try {
            // 步骤 2：执行业务逻辑
            String result = executeBusiness(task, context);

            // 步骤 3：更新为成功状态
            delayJobTaskService.updateTaskStatusToSuccess(taskId);

            log.debug("任务 {} 处理成功，结果: {}", taskId, result);

        } catch (Exception e) {
            // 处理失败：检查是否需要重试
            handleTaskFailure(task, e);
        }
    }

    /**
     * 执行业务逻辑
     */
    private String executeBusiness(DelayJobTask task, JobExecutionContext context) throws Exception {
        log.info("开始执行任务 {}，类型: {}", task.getId(), task.getTaskType());

        // TODO: 根据任务类型执行不同的逻辑
        return switch (task.getTaskType()) {
            case TaskTypeEnum.HTTP ->
                // return httpTaskService.sendRequest(task.getMsgBody(), context.getTraceId());
                    "HTTP 任务执行成功";
            case TaskTypeEnum.MQ ->
                // return kafkaTaskService.sendMessage(task.getMsgBody(), context.getTraceId());
                    "MQ 任务执行成功";
        };
    }

    /**
     * 处理任务失败（包括重试逻辑）
     */
    private void handleTaskFailure(DelayJobTask task, Exception e) {
        long taskId = task.getId();
        int currentRetry = task.getRetryCount();
        int maxRetry = task.getMaxRetryCount();

        if (currentRetry < maxRetry) {
            // 还可以重试
            int nextRetry = currentRetry + 1;
            Instant nextExecuteTime = calculateNextRetryTime(task, nextRetry);

            delayJobTaskService.updateTaskForRetry(taskId, nextRetry, nextExecuteTime);

            long delaySeconds = (nextExecuteTime.toEpochMilli() - System.currentTimeMillis()) / 1000;
            log.info("任务 {} 将在 {} 秒后重试（{}/{}），错误: {}",
                    taskId, delaySeconds, nextRetry, maxRetry, e.getMessage());
        } else {
            // 达到最大重试次数，标记为失败
            delayJobTaskService.updateTaskStatusToFailed(taskId);
            log.error("任务 {} 达到最大重试次数（{}），已标记为失败，最后错误: {}",
                    taskId, maxRetry, e.getMessage());
        }
    }

    /**
     * 计算下次重试时间
     * <p>
     * 支持两种策略：
     * - LINEAR: 第1次延迟 1×基础延迟，第2次 2×基础延迟，第3次 3×基础延迟...
     * - EXPONENTIAL: 第1次延迟 1×基础延迟，第2次 2×基础延迟，第3次 4×基础延迟...
     * - NO_RETRY: 不重试
     * <p>
     * 示例（基础延迟 = 5 秒）：
     * LINEAR:      5秒 → 10秒 → 15秒 → 20秒
     * EXPONENTIAL: 5秒 → 10秒 → 20秒 → 40秒
     */
    private Instant calculateNextRetryTime(DelayJobTask task, int retryCount) {
        long baseDelaySeconds = task.getRetryDelaySeconds();

        // 根据重试策略计算延迟秒数
        long delaySeconds = switch (task.getRetryStrategy()) {
            case RetryStrategyEnum.LINEAR ->
                // 线性递增：基础延迟 × 重试次数
                    baseDelaySeconds * retryCount;
            case RetryStrategyEnum.EXPONENTIAL ->
                // 指数退避：基础延迟 × 2^(重试次数-1)
                    baseDelaySeconds * (long) Math.pow(2, retryCount - 1);
            case RetryStrategyEnum.NO_RETRY ->
                // 不应该执行到这里，但为了安全起见设置为基础延迟
                    baseDelaySeconds;
        };

        // 计算下次执行时间
        Instant now = FmkInstantUtil.now();
        return FmkInstantUtil.plusSeconds(now, delaySeconds);
    }
}