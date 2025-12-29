package com.cjlabs.api.api;

import com.cjlabs.api.business.controller.DelayJobTaskApiService;
import com.cjlabs.api.business.enums.ExecuteTypeEnum;
import com.cjlabs.api.business.enums.HttpMethodEnum;
import com.cjlabs.api.business.enums.TaskTypeEnum;
import com.cjlabs.api.business.requpdate.DelayJobTaskReqSave;
import com.cjlabs.api.business.resp.DelayJobTaskResp;
import com.cjlabs.db.domain.FmkRequest;
import com.cjlabs.web.json.FmkJacksonUtil;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 延迟任务 API 控制器测试
 */
@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
public class Test_BusinessDelayJobTaskController {
    @Autowired
    private DelayJobTaskApiService delayJobTaskApiService;

    /**
     * 测试：创建一个延迟任务
     */
    @Test
    public void testCreateDelayJobTask() throws Exception {
        log.info("========== 测试：创建延迟任务 ==========");

        // 创建请求对象
        DelayJobTaskReqSave reqSave = new DelayJobTaskReqSave();
        reqSave.setTaskType(TaskTypeEnum.HTTP);
        reqSave.setExecuteType(ExecuteTypeEnum.SYNC);
        reqSave.setMsgBody("{\"url\": \"http://example.com\"}");

        reqSave.setHttpUrl("http://example.com");
        reqSave.setHttpHeaders("");
        reqSave.setHttpMethod(HttpMethodEnum.POST);

        FmkRequest<DelayJobTaskReqSave> fmkRequest = new FmkRequest<>();
        fmkRequest.setRequest(reqSave);

        DelayJobTaskResp save = delayJobTaskApiService.save(fmkRequest);
        log.info("Test_BusinessDelayJobTaskController|testCreateDelayJobTask|save={}", FmkJacksonUtil.toJson(save));
    }

}