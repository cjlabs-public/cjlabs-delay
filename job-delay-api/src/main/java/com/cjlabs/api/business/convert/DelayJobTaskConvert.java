package com.cjlabs.api.business.convert;

import com.cjlabs.api.business.mysql.DelayJobTask;
import com.cjlabs.api.business.resp.DelayJobTaskResp;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


public class DelayJobTaskConvert {

    public static DelayJobTaskResp toResp(DelayJobTask input) {
        if (Objects.isNull(input)) {
            return null;
        }
        DelayJobTaskResp delayJobTaskResp = new DelayJobTaskResp();

        delayJobTaskResp.setId(input.getId());
        delayJobTaskResp.setTaskType(input.getTaskType());
        delayJobTaskResp.setExecuteType(input.getExecuteType());
        delayJobTaskResp.setRetryStrategy(input.getRetryStrategy());
        delayJobTaskResp.setRetryDelaySeconds(input.getRetryDelaySeconds());
        delayJobTaskResp.setRetryCount(input.getRetryCount());
        delayJobTaskResp.setMaxRetryCount(input.getMaxRetryCount());
        delayJobTaskResp.setMsgBody(input.getMsgBody());
        delayJobTaskResp.setExecuteTime(input.getExecuteTime());
        delayJobTaskResp.setTaskStatus(input.getTaskStatus());

        delayJobTaskResp.setHttpUrl(input.getHttpUrl());
        delayJobTaskResp.setHttpMethod(input.getHttpMethod());
        delayJobTaskResp.setHttpHeaders(input.getHttpHeaders());
        delayJobTaskResp.setMqTopic(input.getMqTopic());
        delayJobTaskResp.setMqKey(input.getMqKey());
        delayJobTaskResp.setMqPartition(input.getMqPartition());
        delayJobTaskResp.setMqHeaders(input.getMqHeaders());
        delayJobTaskResp.setCreateUser(input.getCreateUser());
        delayJobTaskResp.setCreateDate(input.getCreateDate());
        delayJobTaskResp.setUpdateUser(input.getUpdateUser());
        delayJobTaskResp.setUpdateDate(input.getUpdateDate());

        return delayJobTaskResp;
    }

    public static List<DelayJobTaskResp> toResp(List<DelayJobTask> inputList) {
        if (CollectionUtils.isEmpty(inputList)) {
            return Lists.newArrayList();
        }
        return inputList.stream().map(DelayJobTaskConvert::toResp).collect(Collectors.toList());
    }
}