package com.cjlabs.api.config.job;

import com.cjlabs.boot.job.xxljob.AbstractXxlJobHandler;
import com.cjlabs.web.util.FmkSpringUtil;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Slf4j
@Configuration
public class XxlJobConfig implements ApplicationListener<ApplicationReadyEvent> {
    @Autowired
    private XxlJobProperties xxlJobProperties;

    @Bean
    @ConditionalOnMissingBean(XxlJobSpringExecutor.class)
    @ConditionalOnProperty(prefix = "xxl.job.executor", name = "enabled", havingValue = "true", matchIfMissing = true)
    public XxlJobSpringExecutor xxlJobExecutor() {
        log.info(">>>>>>>>>>> xxl-job config init.");

        XxlJobSpringExecutor xxlJobSpringExecutor = new XxlJobSpringExecutor();
        xxlJobSpringExecutor.setAdminAddresses(xxlJobProperties.getAdmin().getAddresses());
        xxlJobSpringExecutor.setAppname(xxlJobProperties.getExecutor().getAppname());
        xxlJobSpringExecutor.setAddress(xxlJobProperties.getExecutor().getAddress());
        xxlJobSpringExecutor.setIp(xxlJobProperties.getExecutor().getIp());
        xxlJobSpringExecutor.setPort(xxlJobProperties.getExecutor().getPort());
        xxlJobSpringExecutor.setAccessToken(xxlJobProperties.getAdmin().getAccessToken());
        xxlJobSpringExecutor.setLogPath(xxlJobProperties.getExecutor().getLogpath());
        xxlJobSpringExecutor.setLogRetentionDays(xxlJobProperties.getExecutor().getLogretentiondays());

        log.info(">>>>>>>>>>> xxl-job config success. adminAddresses={}, appname={}, port={}",
                xxlJobProperties.getAdmin().getAddresses(),
                xxlJobProperties.getExecutor().getAppname(),
                xxlJobProperties.getExecutor().getPort());

        return xxlJobSpringExecutor;
    }

    /**
     * 自动扫描并注册所有 Job Handler
     * <p>
     * 原理：
     * 1. 从 Spring 容器中获取所有 AbstractXxlJobHandler 类型的 Bean
     * 2. 使用 Bean 的名称作为 JobHandler 名称
     * 3. 自动注册到 XXL-Job 执行器
     * <p>
     * 优点：
     * ✅ 新增 Job 时无需修改配置
     * ✅ 只需要添加 @Component 注解
     * ✅ Bean 名称即为 JobHandler 名称
     */
    private void autoRegisterJobHandlers() {
        // 获取所有 AbstractXxlJobHandler 类型的 Bean
        Map<String, AbstractXxlJobHandler> handlerMap = FmkSpringUtil.getBeansOfType(AbstractXxlJobHandler.class);

        if (handlerMap.isEmpty()) {
            log.warn(">>>>>>>>>>> xxl-job: no job handler found!");
            return;
        }

        // 遍历并注册
        handlerMap.forEach((beanName, handler) -> {
            XxlJobExecutor.registryJobHandler(beanName, handler);
            log.info(">>>>>>>>>>> xxl-job auto register jobhandler success, name:{}, handler:{}",
                    beanName, handler.getClass().getName());
        });

        log.info(">>>>>>>>>>> xxl-job auto register complete, total: {} handlers", handlerMap.size());
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        // 🔥 自动扫描并注册所有继承 AbstractXxlJobHandler 的 Bean
        autoRegisterJobHandlers();
    }
}