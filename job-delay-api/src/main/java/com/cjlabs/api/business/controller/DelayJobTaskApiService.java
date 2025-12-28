package com.cjlabs.api.business.controller;

import com.cjlabs.api.business.convert.DelayJobTaskConvert;
import com.cjlabs.api.business.enums.RetryStrategyEnum;
import com.cjlabs.api.business.enums.TaskStatusEnum;
import com.cjlabs.api.business.enums.TaskTypeEnum;
import com.cjlabs.api.business.mysql.DelayJobTask;
import com.cjlabs.api.business.reqquery.DelayJobTaskReqQuery;
import com.cjlabs.api.business.requpdate.DelayJobTaskReqSave;
import com.cjlabs.api.business.requpdate.DelayJobTaskReqUpdate;
import com.cjlabs.api.business.resp.DelayJobTaskResp;
import com.cjlabs.api.business.service.DelayJobTaskService;
import com.cjlabs.core.time.FmkInstantUtil;
import com.cjlabs.db.domain.FmkPageResponse;
import com.cjlabs.db.domain.FmkRequest;
import com.cjlabs.web.check.FmkCheckUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * delay_job_task 延迟任务配置表
 *
 * 2025-12-28 10:32:02
 */
@Slf4j
@Service
public class DelayJobTaskApiService {
    
    @Autowired
    private DelayJobTaskService delayJobTaskService;
	
    public DelayJobTaskResp getById(FmkRequest<Void> input) {
        // 参数校验
        FmkCheckUtil.checkInput(Objects.isNull(input));
        FmkCheckUtil.checkInput(StringUtils.isBlank(input.getBusinessKey()));

        DelayJobTask delayJobTask = delayJobTaskService.getById(input);
        return DelayJobTaskConvert.toResp(delayJobTask);
    }

    public DelayJobTaskResp save(FmkRequest<DelayJobTaskReqSave> input) {
        // 参数校验
        FmkCheckUtil.checkInput(Objects.isNull(input));
        FmkCheckUtil.checkInput(Objects.isNull(input.getRequest()));

        DelayJobTaskReqSave request = input.getRequest();
        if (Objects.isNull(request)) {
            log.info("DelayJobTaskApiService|save|request is null");
            return null;
        }

        Instant executeTime = request.getExecuteTime();
        if (Objects.isNull(executeTime)) {
            log.info("DelayJobTaskApiService|save|executeTime is null");
            Instant now = FmkInstantUtil.now();
            request.setExecuteTime(now);
        }

        request.setTaskType(TaskTypeEnum.HTTP);
        request.setRetryStrategy(RetryStrategyEnum.LINEAR);
        // 3s
        request.setRetryDelaySeconds(3);
        request.setRetryCount(0);
        request.setMaxRetryCount(5);
        request.setTaskStatus(TaskStatusEnum.PENDING);

        DelayJobTask delayJobTask = delayJobTaskService.save(request);
        return DelayJobTaskConvert.toResp(delayJobTask);
    }


    public boolean update(FmkRequest<DelayJobTaskReqUpdate> input) {
        // 参数校验
        FmkCheckUtil.checkInput(Objects.isNull(input));
        FmkCheckUtil.checkInput(Objects.isNull(input.getRequest()));

        DelayJobTaskReqUpdate request = input.getRequest();
        if (Objects.isNull(request)) {
            log.info("DelayJobTaskApiService|update|request is null");
            return false;
        }
        return delayJobTaskService.update(request);
    }

    public boolean deleteById(FmkRequest<Void> input) {
        // 参数校验
        FmkCheckUtil.checkInput(Objects.isNull(input));
        FmkCheckUtil.checkInput(Objects.isNull(input.getBusinessKey()));

        String businessKey = input.getBusinessKey();
        if (businessKey == null) {
        	log.info("DelayJobTaskApiService|deleteById|request is null");
            return false;
        }
        return delayJobTaskService.deleteById(businessKey);
    }

    /**
     * 查询所有（不分页）
     */
    public List<DelayJobTaskResp> listAll() {
        List<DelayJobTask> entityList = delayJobTaskService.listAll();
        List<DelayJobTaskResp> respList = DelayJobTaskConvert.toResp(entityList);
        return respList;
    }
    
      /**
     * 分页查询
     */
    public FmkPageResponse<DelayJobTaskResp> pageQuery(FmkRequest<DelayJobTaskReqQuery> input) {
        // 参数校验
        FmkCheckUtil.checkInput(Objects.isNull(input));
        FmkCheckUtil.checkInput(Objects.isNull(input.getRequest()));

        // 执行分页查询
        FmkPageResponse<DelayJobTask> entityPage = delayJobTaskService.pageQuery(input);

        if (Objects.isNull(entityPage) || CollectionUtils.isEmpty(entityPage.getRecords())) {
            return FmkPageResponse.empty();
        }

        FmkPageResponse<DelayJobTaskResp> pageResponse = FmkPageResponse.of(entityPage, DelayJobTaskConvert::toResp);

        return pageResponse;
    }
}