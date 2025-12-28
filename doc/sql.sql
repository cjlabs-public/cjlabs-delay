create database if not exists job_delay_scheduler;

use job_delay_scheduler;

-- ===================== 1. 任务配置表 =====================
-- ===================== 延迟任务配置表 - 完整建表语句 =====================
create table delay_job_task
(
    id                  bigint auto_increment comment '主键ID'
        primary key,
    task_type           enum ('HTTP', 'MQ')                                             default 'HTTP'    not null comment '任务类型：HTTP-HTTP调用，MQ-消息队列',
    execute_type        enum ('SYNC', 'ASYNC')                                          default 'ASYNC'   not null comment '执行方式：SYNC-同步，ASYNC-异步',
    retry_strategy      enum ('NO_RETRY', 'LINEAR', 'EXPONENTIAL')                      default 'LINEAR'  not null comment '重试策略：NO_RETRY-不重试，LINEAR-线性递增，EXPONENTIAL-指数退避',
    retry_delay_seconds int                                                             default 5000      not null comment '重试延迟时间（毫秒）',
    retry_count         int                                                             default 0         not null comment '当前重试次数',
    max_retry_count     int                                                             default 3         not null comment '最大重试次数',
    msg_body            varchar(2048)                                                   default ''        not null comment '消息体 / HTTP Request Body',
    execute_time        bigint                                                                            not null comment '执行时间',
    task_status         enum ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED', 'CANCELED') default 'PENDING' not null comment '任务状态',

    -- ===== HTTP 任务专用字段 =====
    http_url            varchar(128)                                                                      null comment 'HTTP请求URL',
    http_method         enum ('POST', 'GET')                                            default 'POST'    null comment 'HTTP方法：GET,POST',
    http_headers        varchar(512)                                                                      null comment 'HTTP请求头（JSON格式）',

    -- ===== MQ 任务专用字段 =====
    mq_topic            varchar(64)                                                                       null comment 'Kafka Topic',
    mq_key              varchar(64)                                                                       null comment 'Kafka消息Key（用于分区和消息去重）',
    mq_partition        varchar(8)                                                                        null comment 'Kafka分区号',
    mq_headers          varchar(512)                                                                      null comment 'Kafka消息头（JSON格式）',

    -- ===== 基础字段 =====
    del_flag            enum ('NORMAL', 'ABNORMAL')                                     default 'NORMAL'  not null comment '删除标志',
    create_user         varchar(64)                                                                       null comment '创建用户',
    create_date         bigint                                                                            not null comment '创建时间',
    update_user         varchar(64)                                                                       null comment '更新用户',
    update_date         bigint                                                                            not null comment '更新时间',
    trace_id            varchar(64)                                                                       null comment '追踪ID'
)
    comment '延迟任务配置表' collate = utf8mb4_unicode_ci;

-- ===== 索引设计 =====
-- 1. 删除标志索引（过滤逻辑删除）
create index idx_del_flag
    on delay_job_task (del_flag);

-- 2. 任务状态索引（快速查询待执行任务）
create index idx_next_execute
    on delay_job_task (task_status);

-- 3. 扫描任务复合索引（支持分片查询和状态过滤）
create index idx_scan_tasks
    on delay_job_task (task_status, execute_time, del_flag);

-- 6. 按任务类型和状态查询
create index idx_task_type_status
    on delay_job_task (task_type, task_status, del_flag);


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



