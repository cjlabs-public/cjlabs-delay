package com.cjlabs.api.business.mysql;

import com.cjlabs.api.business.enums.ExecuteStatusEnum;
import com.cjlabs.db.domain.FmkBaseEntity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * delay_job_execute_log 延迟任务执行日志表
 * <p>
 * 2025-12-28 10:32:02
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class DelayJobExecuteLog extends FmkBaseEntity {

    /**
     * 关联的任务实例ID
     */
    private Long taskId;

    /**
     * 执行序号（第几次执行，包括重试）
     */
    private Integer executeSeq;

    /**
     * 执行时间（时间戳）
     */
    private Instant executeTime;

    /**
     * 执行耗时（毫秒）
     */
    private Integer durationMs;

    /**
     * 执行状态
     */
    private ExecuteStatusEnum executeStatus;

    /**
     * 执行结果（JSON格式）
     */
    private String executeResult;

    /**
     * 处理机器IP
     */
    private String handlerIp;

    /**
     * XXL-Job 的 Log ID
     */
    private Long xxlJobLogId;

}