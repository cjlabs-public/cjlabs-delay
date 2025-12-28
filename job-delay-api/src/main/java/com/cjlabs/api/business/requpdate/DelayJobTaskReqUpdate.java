package com.cjlabs.api.business.requpdate;

import com.cjlabs.api.business.enums.ExecuteTypeEnum;
import com.cjlabs.api.business.enums.RetryStrategyEnum;
import com.cjlabs.api.business.enums.TaskStatusEnum;
import com.cjlabs.api.business.enums.TaskTypeEnum;
import lombok.Data;

import java.time.Instant;

/**
 * delay_job_task 延迟任务配置表
 * <p>
 * 2025-12-28 10:32:02
 */
@Data
public class DelayJobTaskReqUpdate {

    /**
     * 主键ID
     */
    private Long id;

    /**
     * 任务类型：HTTP-HTTP调用，
     */
    private TaskTypeEnum taskType;

    /**
     * 执行方式：SYNC-同步，ASYNC-异步
     */
    private ExecuteTypeEnum executeType;

    /**
     * 重试策略：NO_RETRY-不重试，LINEAR-线性递增，EXPONENTIAL-指数退避
     */
    private RetryStrategyEnum retryStrategy;

    /**
     * 重试延迟时间（毫秒）
     */
    private Integer retryDelaySeconds;

    /**
     * 当前重试次数
     */
    private Integer retryCount;

    /**
     * 最大重试次数
     */
    private Integer maxRetryCount;

    /**
     * 消息体
     */
    private String msgBody;

    /**
     * 执行时间
     */
    private Instant executeTime;

    /**
     * 任务状态
     */
    private TaskStatusEnum taskStatus;

    /**
     * HTTP请求URL
     */
    private String httpUrl;

    /**
     * HTTP方法：GET,POST
     */
    private String httpMethod;

    /**
     * HTTP请求头（JSON格式）
     */
    private String httpHeaders;

    /**
     * Kafka Topic
     */
    private String mqTopic;

    /**
     * Kafka消息Key（用于分区和消息去重）
     */
    private String mqKey;

    /**
     * Kafka分区号
     */
    private String mqPartition;

    /**
     * Kafka消息头（JSON格式）
     */
    private String mqHeaders;
}