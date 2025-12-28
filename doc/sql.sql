create database if not exists job_delay_scheduler;

use job_delay_scheduler;

-- ===================== 1. 任务配置表 =====================
CREATE TABLE `delay_job_task`
(
    `id`                  BIGINT AUTO_INCREMENT COMMENT '主键ID',

    -- 任务执行相关配置
    `task_type`           ENUM ('HTTP', 'MQ')                                             DEFAULT 'HTTP'    NOT NULL COMMENT '任务类型：HTTP-HTTP调用，',
    `execute_type`        ENUM ('SYNC', 'ASYNC')                                          DEFAULT 'ASYNC'   NOT NULL COMMENT '执行方式：SYNC-同步，ASYNC-异步',
    `retry_strategy`      ENUM ('NO_RETRY', 'LINEAR', 'EXPONENTIAL')                      DEFAULT 'LINEAR'  NOT NULL COMMENT '重试策略：NO_RETRY-不重试，LINEAR-线性递增，EXPONENTIAL-指数退避',
    `retry_delay_seconds` INT                                                             DEFAULT 5000      NOT NULL COMMENT '重试延迟时间（毫秒）',

    `retry_count`         INT                                                             DEFAULT 0         NOT NULL COMMENT '当前重试次数',
    `max_retry_count`     INT                                                             DEFAULT 3         NOT NULL COMMENT '最大重试次数',
    `msg_body`            varchar(2048)                                                   default ''        NOT NULL COMMENT '消息体',
    `execute_time`        BIGINT                                                                            NOT NULL COMMENT '执行时间',
    `task_status`         ENUM ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED', 'CANCELED') DEFAULT 'PENDING' NOT NULL COMMENT '任务状态',

    -- 基础字段
    `del_flag`            ENUM ('NORMAL', 'ABNORMAL')                                     DEFAULT 'NORMAL'  NOT NULL COMMENT '删除标志',
    `create_user`         VARCHAR(64)                                                                       NULL COMMENT '创建用户',
    `create_date`         BIGINT                                                                            NOT NULL COMMENT '创建时间',
    `update_user`         VARCHAR(64)                                                                       NULL COMMENT '更新用户',
    `update_date`         BIGINT                                                                            NOT NULL COMMENT '更新时间',
    `trace_id`            VARCHAR(64)                                                                       NULL COMMENT '追踪ID',

    PRIMARY KEY (`id`),
    INDEX `idx_del_flag` (`del_flag`),
    INDEX `idx_scan_tasks` (`task_status`, `execute_time`, `del_flag`),
    INDEX `idx_next_execute` (`task_status`)
) COMMENT ='延迟任务配置表' COLLATE = utf8mb4_unicode_ci
                            ENGINE = InnoDB
                            DEFAULT CHARSET = utf8mb4;


-- ===================== 3. 任务执行日志表 =====================
CREATE TABLE `delay_job_execute_log`
(
    `id`             BIGINT AUTO_INCREMENT COMMENT '主键ID',
    `task_id`        BIGINT                                                       NOT NULL COMMENT '关联的任务实例ID',

    -- 执行信息
    `execute_seq`    INT                                                          NOT NULL COMMENT '执行序号（第几次执行，包括重试）',
    `execute_time`   BIGINT                                                       NOT NULL COMMENT '执行时间（时间戳）',
    `duration_ms`    INT                                                          NULL COMMENT '执行耗时（毫秒）',

    -- 执行结果
    `execute_status` ENUM ('RUNNING', 'SUCCESS', 'FAILED', 'TIMEOUT', 'CANCELED') NOT NULL COMMENT '执行状态',
    `execute_result` LONGTEXT                                                     NULL COMMENT '执行结果（JSON格式）',

    -- 处理机器信息
    `handler_ip`     VARCHAR(50)                                                  NULL COMMENT '处理机器IP',
    `xxl_job_log_id` BIGINT                                                       NULL COMMENT 'XXL-Job 的 Log ID',

    -- 基础字段
    `del_flag`       ENUM ('NORMAL', 'ABNORMAL') DEFAULT 'NORMAL'                 NOT NULL COMMENT '删除标志',
    `create_user`    VARCHAR(64)                                                  NULL COMMENT '创建用户',
    `create_date`    BIGINT                                                       NOT NULL COMMENT '创建时间',
    `update_user`    VARCHAR(64)                                                  NULL COMMENT '更新用户',
    `update_date`    BIGINT                                                       NOT NULL COMMENT '更新时间',
    `trace_id`       VARCHAR(64)                                                  NULL COMMENT '追踪ID',

    PRIMARY KEY (`id`),
    INDEX `idx_task_id` (`task_id`),
    INDEX `idx_task_execute` (`task_id`, `execute_seq`),
    INDEX `idx_execute_time` (`execute_time`),
    INDEX `idx_execute_status` (`execute_status`)
) COMMENT ='延迟任务执行日志表' COLLATE = utf8mb4_unicode_ci
                                ENGINE = InnoDB
                                DEFAULT CHARSET = utf8mb4;



