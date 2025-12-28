package com.cjlabs.api.business.mapper;

import com.cjlabs.api.business.enums.TaskStatusEnum;
import com.cjlabs.api.business.mysql.DelayJobTask;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cjlabs.core.types.longs.FmkUserId;
import com.cjlabs.core.types.strings.FmkTraceId;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.List;

/**
 * delay_job_task 延迟任务配置表
 * <p>
 * 2025-12-28 10:32:02
 */
@Mapper
public interface DelayJobTaskMapper extends BaseMapper<DelayJobTask> {

    /**
     * 获取当前最大 ID（用于分片范围计算）
     * 性能：极快，因为 id 是主键
     */
    Long getMaxId();

    /**
     * 按分片范围查询待执行任务（完全走索引）
     * <p>
     * 索引：(task_status, execute_time, id)
     *
     * @param now      当前时间戳
     * @param idStart  分片起始 ID
     * @param idEnd    分片结束 ID
     * @param offset   分页偏移
     * @param pageSize 分页大小
     */
    List<DelayJobTask> queryPendingTasksByShardRange(
            @Param("now") Instant now,
            @Param("idStart") long idStart,
            @Param("idEnd") long idEnd,
            @Param("offset") int offset,
            @Param("pageSize") int pageSize
    );

    /**
     * 原子性更新状态 PENDING -> PROCESSING（防重复）
     * 防止重复处理
     */
    int updateTaskStatusToProcessing(
            @Param("taskId") long taskId,
            @Param("updateUser") FmkUserId updateUser,
            @Param("now") Instant now,
            @Param("traceId") FmkTraceId traceId
    );

}