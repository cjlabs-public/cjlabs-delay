package com.cjlabs.api.business.convert;

import com.cjlabs.api.business.mysql.DelayJobExecuteLog;
import com.cjlabs.api.business.reqquery.DelayJobExecuteLogReqQuery;
import com.cjlabs.api.business.requpdate.DelayJobExecuteLogReqSave;
import com.cjlabs.api.business.requpdate.DelayJobExecuteLogReqUpdate;

import java.util.Objects;

public class DelayJobExecuteLogReqConvert {

    public static DelayJobExecuteLog toDb(DelayJobExecuteLogReqQuery input) {
        if (Objects.isNull(input)) {
            return null;
        }
        DelayJobExecuteLog delayJobExecuteLog = new DelayJobExecuteLog();

        delayJobExecuteLog.setTaskId(input.getTaskId());
        delayJobExecuteLog.setExecuteSeq(input.getExecuteSeq());
        delayJobExecuteLog.setExecuteTime(input.getExecuteTime());
        delayJobExecuteLog.setDurationMs(input.getDurationMs());
        delayJobExecuteLog.setExecuteStatus(input.getExecuteStatus());
        delayJobExecuteLog.setExecuteResult(input.getExecuteResult());
        delayJobExecuteLog.setHandlerIp(input.getHandlerIp());
        delayJobExecuteLog.setXxlJobLogId(input.getXxlJobLogId());

        return delayJobExecuteLog;
    }

    public static DelayJobExecuteLog toDb(DelayJobExecuteLogReqUpdate input) {
        if (Objects.isNull(input)) {
            return null;
        }
        DelayJobExecuteLog delayJobExecuteLog = new DelayJobExecuteLog();

        delayJobExecuteLog.setId(input.getId());
        delayJobExecuteLog.setTaskId(input.getTaskId());
        delayJobExecuteLog.setExecuteSeq(input.getExecuteSeq());
        delayJobExecuteLog.setExecuteTime(input.getExecuteTime());
        delayJobExecuteLog.setDurationMs(input.getDurationMs());
        delayJobExecuteLog.setExecuteStatus(input.getExecuteStatus());
        delayJobExecuteLog.setExecuteResult(input.getExecuteResult());
        delayJobExecuteLog.setHandlerIp(input.getHandlerIp());
        delayJobExecuteLog.setXxlJobLogId(input.getXxlJobLogId());

        return delayJobExecuteLog;
    }

    public static DelayJobExecuteLog toDb(DelayJobExecuteLogReqSave input) {
        if (Objects.isNull(input)) {
            return null;
        }
        DelayJobExecuteLog delayJobExecuteLog = new DelayJobExecuteLog();

        delayJobExecuteLog.setTaskId(input.getTaskId());
        delayJobExecuteLog.setExecuteSeq(input.getExecuteSeq());
        delayJobExecuteLog.setExecuteTime(input.getExecuteTime());
        delayJobExecuteLog.setDurationMs(input.getDurationMs());
        delayJobExecuteLog.setExecuteStatus(input.getExecuteStatus());
        delayJobExecuteLog.setExecuteResult(input.getExecuteResult());
        delayJobExecuteLog.setHandlerIp(input.getHandlerIp());
        delayJobExecuteLog.setXxlJobLogId(input.getXxlJobLogId());

        return delayJobExecuteLog;
    }
}