package com.cjlabs.api.business.mapper;

import com.cjlabs.api.business.mysql.DelayJobTask;
import com.cjlabs.api.business.reqquery.DelayJobTaskReqQuery;
import com.cjlabs.core.time.FmkInstantUtil;
import com.cjlabs.core.types.longs.FmkUserId;
import com.cjlabs.core.types.strings.FmkTraceId;
import com.cjlabs.db.mp.FmkService;
import com.cjlabs.db.domain.FmkOrderItem;
import com.cjlabs.db.domain.FmkPageResponse;
import com.cjlabs.db.domain.FmkRequest;
import com.cjlabs.web.check.FmkCheckUtil;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * delay_job_task 延迟任务配置表
 * <p>
 * 2025-12-28 10:32:02
 */
@Slf4j
@Service
public class DelayJobTaskWrapMapper extends FmkService<DelayJobTaskMapper, DelayJobTask> {

    protected DelayJobTaskWrapMapper(DelayJobTaskMapper mapper) {
        super(mapper);
    }

    @Override
    protected Class<DelayJobTask> getEntityClass() {
        return DelayJobTask.class;
    }

    /**
     * 分页查询
     */
    public FmkPageResponse<DelayJobTask> pageQuery(FmkRequest<DelayJobTaskReqQuery> input) {
        // 参数校验
        FmkCheckUtil.checkInput(Objects.isNull(input));
        FmkCheckUtil.checkInput(Objects.isNull(input.getRequest()));

        // 构建分页对象
        Page<DelayJobTask> page = new Page<>(input.getCurrent(), input.getSize());
        DelayJobTaskReqQuery request = input.getRequest();

        // 构建查询条件
        LambdaQueryWrapper<DelayJobTask> lambdaQuery = buildLambdaQuery();


        List<FmkOrderItem> orderItemList = input.getOrderItemList();

        // 执行分页查询
        IPage<DelayJobTask> dbPage = super.pageByCondition(page, lambdaQuery, orderItemList);

        return FmkPageResponse.of(dbPage);
    }

    /**
     * 分页查询
     */
    public Long getMaxId() {
        return getBaseMapper().getMaxId();
    }

    /**
     * 分片分页查询待执行任务
     * <p>
     * 查询逻辑：
     * - 只查询 task_status = 'PENDING' 的任务
     * - 只查询 execute_time <= 当前时间 的任务
     * - 只查询 del_flag = 'NORMAL' 的任务（排除已删除）
     * - 按分片范围 [idStart, idEnd) 过滤
     * - 完全走索引，性能最优
     *
     * @param idStart  分片起始 ID（包括）
     * @param idEnd    分片结束 ID（不包括）
     * @param pageNum  页码（从 1 开始）
     * @param pageSize 每页大小
     * @return 待执行的任务列表
     */
    public List<DelayJobTask> queryPendingTasksByShardAndPage(long idStart, long idEnd, int pageNum, int pageSize) {

        int offset = (pageNum - 1) * pageSize;

        log.debug("查询待执行任务: idStart={}, idEnd={}, pageNum={}, offset={}, pageSize={}",
                idStart, idEnd, pageNum, offset, pageSize);

        Instant now = FmkInstantUtil.now();

        return getBaseMapper().queryPendingTasksByShardRange(now, idStart, idEnd, offset, pageSize);
    }

    /**
     * 原子性更新任务状态为 PROCESSING
     * 防止重复处理
     *
     * @param taskId     任务 ID
     * @return 更新行数（1 表示成功，0 表示已被其他实例处理）
     */
    public int updateTaskStatusToProcessing(long taskId) {

        log.debug("原子性更新任务状态 PENDING -> PROCESSING: taskId={}", taskId);

        FmkUserId system = FmkUserId.SYSTEM;
        Instant now = FmkInstantUtil.now();
        FmkTraceId traceId = FmkTraceId.generate();

        return getBaseMapper().updateTaskStatusToProcessing(taskId, system, now, traceId);
    }
}