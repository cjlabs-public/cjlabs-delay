package com.cjlabs.api.business.convert;

import com.cjlabs.api.business.mysql.DelayJobTask;
import com.cjlabs.api.business.reqquery.DelayJobTaskReqQuery;
import com.cjlabs.api.business.requpdate.DelayJobTaskReqSave;
import com.cjlabs.api.business.requpdate.DelayJobTaskReqUpdate;

import java.util.Objects;

public class DelayJobTaskReqConvert {

    public static DelayJobTask toDb(DelayJobTaskReqQuery input) {
        if (Objects.isNull(input)) {
            return null;
        }
        DelayJobTask delayJobTask = new DelayJobTask();

        delayJobTask.setTaskType(input.getTaskType());
        delayJobTask.setExecuteType(input.getExecuteType());
        delayJobTask.setRetryStrategy(input.getRetryStrategy());
        delayJobTask.setRetryDelaySeconds(input.getRetryDelaySeconds());
        delayJobTask.setRetryCount(input.getRetryCount());
        delayJobTask.setMaxRetryCount(input.getMaxRetryCount());
        delayJobTask.setMsgBody(input.getMsgBody());
        delayJobTask.setExecuteTime(input.getExecuteTime());
        delayJobTask.setTaskStatus(input.getTaskStatus());

        delayJobTask.setHttpUrl(input.getHttpUrl());
        delayJobTask.setHttpMethod(input.getHttpMethod());
        delayJobTask.setHttpHeaders(input.getHttpHeaders());
        delayJobTask.setMqTopic(input.getMqTopic());
        delayJobTask.setMqKey(input.getMqKey());
        delayJobTask.setMqPartition(input.getMqPartition());
        delayJobTask.setMqHeaders(input.getMqHeaders());

        return delayJobTask;
    }

    public static DelayJobTask toDb(DelayJobTaskReqUpdate input) {
        if (Objects.isNull(input)) {
            return null;
        }
        DelayJobTask delayJobTask = new DelayJobTask();

        delayJobTask.setId(input.getId());
        delayJobTask.setTaskType(input.getTaskType());
        delayJobTask.setExecuteType(input.getExecuteType());
        delayJobTask.setRetryStrategy(input.getRetryStrategy());
        delayJobTask.setRetryDelaySeconds(input.getRetryDelaySeconds());
        delayJobTask.setRetryCount(input.getRetryCount());
        delayJobTask.setMaxRetryCount(input.getMaxRetryCount());
        delayJobTask.setMsgBody(input.getMsgBody());
        delayJobTask.setExecuteTime(input.getExecuteTime());
        delayJobTask.setTaskStatus(input.getTaskStatus());

        delayJobTask.setHttpUrl(input.getHttpUrl());
        delayJobTask.setHttpMethod(input.getHttpMethod());
        delayJobTask.setHttpHeaders(input.getHttpHeaders());
        delayJobTask.setMqTopic(input.getMqTopic());
        delayJobTask.setMqKey(input.getMqKey());
        delayJobTask.setMqPartition(input.getMqPartition());
        delayJobTask.setMqHeaders(input.getMqHeaders());

        return delayJobTask;
    }

    public static DelayJobTask toDb(DelayJobTaskReqSave input) {
        if (Objects.isNull(input)) {
            return null;
        }
        DelayJobTask delayJobTask = new DelayJobTask();

        delayJobTask.setTaskType(input.getTaskType());
        delayJobTask.setExecuteType(input.getExecuteType());
        delayJobTask.setRetryStrategy(input.getRetryStrategy());
        delayJobTask.setRetryDelaySeconds(input.getRetryDelaySeconds());
        delayJobTask.setRetryCount(input.getRetryCount());
        delayJobTask.setMaxRetryCount(input.getMaxRetryCount());
        delayJobTask.setMsgBody(input.getMsgBody());
        delayJobTask.setExecuteTime(input.getExecuteTime());
        delayJobTask.setTaskStatus(input.getTaskStatus());

        delayJobTask.setHttpUrl(input.getHttpUrl());
        delayJobTask.setHttpMethod(input.getHttpMethod());
        delayJobTask.setHttpHeaders(input.getHttpHeaders());
        delayJobTask.setMqTopic(input.getMqTopic());
        delayJobTask.setMqKey(input.getMqKey());
        delayJobTask.setMqPartition(input.getMqPartition());
        delayJobTask.setMqHeaders(input.getMqHeaders());

        return delayJobTask;
    }
}