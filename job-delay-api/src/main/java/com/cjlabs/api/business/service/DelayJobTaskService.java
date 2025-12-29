package com.cjlabs.api.business.service;

import com.cjlabs.api.business.convert.DelayJobTaskReqConvert;
import com.cjlabs.api.business.enums.TaskStatusEnum;
import com.cjlabs.api.business.mapper.DelayJobTaskWrapMapper;
import com.cjlabs.api.business.mysql.DelayJobTask;
import com.cjlabs.api.business.reqquery.DelayJobTaskReqQuery;
import com.cjlabs.api.business.requpdate.DelayJobTaskReqSave;
import com.cjlabs.api.business.requpdate.DelayJobTaskReqUpdate;
import com.cjlabs.db.datasource.FmkTransactionTemplateUtil;
import com.cjlabs.db.domain.FmkPageResponse;
import com.cjlabs.db.domain.FmkRequest;
import com.cjlabs.web.check.FmkCheckUtil;
import com.cjlabs.domain.exception.Error200ExceptionEnum;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
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
public class DelayJobTaskService {

    @Autowired
    private DelayJobTaskWrapMapper delayJobTaskWrapMapper;
    @Autowired
    private FmkTransactionTemplateUtil fmkTransactionTemplateUtil;

    public DelayJobTask getById(FmkRequest<Void> input) {
        // 参数校验
        FmkCheckUtil.checkInput(Objects.isNull(input));
        FmkCheckUtil.checkInput(StringUtils.isBlank(input.getBusinessKey()));

        String id = input.getBusinessKey();
        return delayJobTaskWrapMapper.getById(id);
    }

    public DelayJobTask save(DelayJobTaskReqSave request) {
        FmkCheckUtil.checkInput(Objects.isNull(request));

        DelayJobTask db = DelayJobTaskReqConvert.toDb(request);

        int saved = delayJobTaskWrapMapper.save(db);
        FmkCheckUtil.throw200Error(saved == 0, Error200ExceptionEnum.DATA_NOT_FOUND);
        return db;
    }


    public boolean update(DelayJobTaskReqUpdate request) {
        FmkCheckUtil.checkInput(Objects.isNull(request));

        DelayJobTask db = DelayJobTaskReqConvert.toDb(request);

        int updated = delayJobTaskWrapMapper.updateById(db);
        if (updated > 0) {
            log.info("DelayJobTaskService|update|update={}|id={}", updated, request.getId());
            return true;
        }
        return false;
    }

    public boolean deleteById(String businessKey) {
        // 参数校验
        FmkCheckUtil.checkInput(StringUtils.isBlank(businessKey));

        int deleted = delayJobTaskWrapMapper.deleteById(businessKey);
        if (deleted > 0) {
            log.info("DelayJobTaskService|deleteById|deleteById={}|id={}", deleted, businessKey);
            return true;
        }
        return false;
    }

    /**
     * 查询所有（不分页）
     */
    public List<DelayJobTask> listAll() {
        List<DelayJobTask> entityList = delayJobTaskWrapMapper.listAllLimitService();
        return entityList;
    }

    /**
     * 分页查询
     */
    public FmkPageResponse<DelayJobTask> pageQuery(FmkRequest<DelayJobTaskReqQuery> input) {
        // 参数校验
        FmkCheckUtil.checkInput(Objects.isNull(input));
        FmkCheckUtil.checkInput(Objects.isNull(input.getRequest()));

        // 执行分页查询
        FmkPageResponse<DelayJobTask> entityPage = delayJobTaskWrapMapper.pageQuery(input);

        return entityPage;
    }


    /**
     * 获取当前最大 ID
     * 用于计算分片范围
     */
    public Long getMaxId() {
        Long maxId = delayJobTaskWrapMapper.getMaxId();
        return maxId != null ? maxId : 0;
    }

    /**
     * 计算分片的 ID 范围
     * <p>
     * 算法：将 0 到 maxId 均匀分成 shardTotal 份
     * 每个分片负责其中的一份
     * <p>
     * 示例（maxId=10M, shardTotal=4）:
     * 分片 0: [0, 2.5M)
     * 分片 1: [2.5M, 5M)
     * 分片 2: [5M, 7.5M)
     * 分片 3: [7.5M, 10M]  ← 注意：最后一个分片包括 maxId
     */
    public long[] getShardIdRange(int shardIndex, int shardTotal, long maxId) {
        if (maxId <= 0) {
            return new long[]{0, 0};
        }

        // 计算每个分片的大小
        long rangeSize = (maxId + shardTotal - 1) / shardTotal;

        long idStart = shardIndex * rangeSize;
        long idEnd = (shardIndex + 1) * rangeSize;

        // 🔥 关键修改：最后一个分片的结束是 maxId + 1，确保包含 maxId
        if (shardIndex == shardTotal - 1) {
            idEnd = maxId + 1;  // ✅ 修改为 maxId + 1
        }

        log.debug("分片 {}/{} ID 范围: [{}, {})", shardIndex, shardTotal, idStart, idEnd);

        return new long[]{idStart, idEnd};
    }

    /**
     * 分片分页查询待执行任务
     * <p>
     * 关键点：
     * - 动态获取 maxId
     * - 计算分片范围
     * - 按范围查询（走索引）
     */
    public List<DelayJobTask> queryPendingTasksByShardAndPage(
            int shardIndex, int shardTotal, int pageNum, int pageSize) {

        // 动态获取最大 ID
        long maxId = getMaxId();

        // 计算该分片的 ID 范围
        long[] range = getShardIdRange(shardIndex, shardTotal, maxId);
        long idStart = range[0];
        long idEnd = range[1];

        log.debug("[分片 {}/{}] 分片范围: [{}, {}], 页码: {}, 每页: {}",
                shardIndex, shardTotal, idStart, idEnd, pageNum, pageSize);

        return delayJobTaskWrapMapper.queryPendingTasksByShardAndPage(
                idStart, idEnd, pageNum, pageSize
        );
    }

    /**
     * 原子性更新任务状态为 PROCESSING
     * <p>
     * 返回：
     * - true: 更新成功，该任务被锁定，可以处理
     * - false: 更新失败，任务已被其他实例处理
     */
    public boolean updateTaskStatusToProcessing(long taskId) {

        return fmkTransactionTemplateUtil.executeTx(() -> {

            int updated = delayJobTaskWrapMapper.updateTaskStatusToProcessing(taskId);

            if (updated > 0) {
                log.debug("任务 {} 状态已更新为 PROCESSING", taskId);
                return true;
            } else {
                log.debug("任务 {} 已被其他实例处理，跳过", taskId);
                return false;
            }

        });
    }


    /**
     * 更新任务为成功状态
     */
    public void updateTaskStatusToSuccess(long taskId) {
        fmkTransactionTemplateUtil.executeTx(() -> {

            DelayJobTask task = new DelayJobTask();
            task.setId(taskId);
            task.setTaskStatus(TaskStatusEnum.SUCCESS);
            delayJobTaskWrapMapper.updateById(task);

        });

        log.debug("任务 {} 状态已更新为 SUCCESS", taskId);
    }

    public void updateTaskStatusToFailed(long taskId) {

        fmkTransactionTemplateUtil.executeTx(() -> {

            DelayJobTask task = new DelayJobTask();
            task.setId(taskId);
            task.setTaskStatus(TaskStatusEnum.FAILED);
            delayJobTaskWrapMapper.updateById(task);

        });

        log.error("任务 {} 状态已更新为 FAILED", taskId);

    }


    /**
     * 更新任务为待重试状态
     *
     * @param taskId          任务 ID
     * @param newRetryCount   新的重试次数
     * @param nextExecuteTime 下一次执行时间
     */
    public void updateTaskForRetry(long taskId, int newRetryCount, Instant nextExecuteTime) {

        fmkTransactionTemplateUtil.executeTx(() -> {

            DelayJobTask task = new DelayJobTask();
            task.setId(taskId);
            task.setRetryCount(newRetryCount);
            task.setTaskStatus(TaskStatusEnum.PENDING);
            task.setExecuteTime(nextExecuteTime);
            delayJobTaskWrapMapper.updateById(task);

        });

        log.debug("任务 {} 更新为待重试状态，重试次数: {}", taskId, newRetryCount);
    }
}