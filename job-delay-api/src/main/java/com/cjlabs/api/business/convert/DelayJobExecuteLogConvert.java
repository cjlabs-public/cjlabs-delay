package com.cjlabs.api.business.convert;

import com.cjlabs.api.business.mysql.DelayJobExecuteLog;
import com.cjlabs.api.business.resp.DelayJobExecuteLogResp;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


public class DelayJobExecuteLogConvert {

    public static DelayJobExecuteLogResp toResp(DelayJobExecuteLog input) {
        if (Objects.isNull(input)) {
            return null;
        }
        DelayJobExecuteLogResp delayJobExecuteLogResp = new DelayJobExecuteLogResp();

        delayJobExecuteLogResp.setId(input.getId());
        delayJobExecuteLogResp.setTaskId(input.getTaskId());
        delayJobExecuteLogResp.setExecuteSeq(input.getExecuteSeq());
        delayJobExecuteLogResp.setExecuteTime(input.getExecuteTime());
        delayJobExecuteLogResp.setDurationMs(input.getDurationMs());
        delayJobExecuteLogResp.setExecuteStatus(input.getExecuteStatus());
        delayJobExecuteLogResp.setExecuteResult(input.getExecuteResult());
        delayJobExecuteLogResp.setHandlerIp(input.getHandlerIp());
        delayJobExecuteLogResp.setXxlJobLogId(input.getXxlJobLogId());
        delayJobExecuteLogResp.setCreateUser(input.getCreateUser());
        delayJobExecuteLogResp.setCreateDate(input.getCreateDate());
        delayJobExecuteLogResp.setUpdateUser(input.getUpdateUser());
        delayJobExecuteLogResp.setUpdateDate(input.getUpdateDate());

        return delayJobExecuteLogResp;
    }

    public static List<DelayJobExecuteLogResp> toResp(List<DelayJobExecuteLog> inputList) {
        if (CollectionUtils.isEmpty(inputList)) {
            return Lists.newArrayList();
        }
        return inputList.stream().map(DelayJobExecuteLogConvert::toResp).collect(Collectors.toList());
    }
}