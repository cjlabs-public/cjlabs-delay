package com.cjlabs.api;

import com.cjlabs.boot.runner.ApplicationContextRunnerWrapper;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Job Delay Scheduler 主启动类
 */
@SpringBootApplication
@EnableDiscoveryClient
@MapperScan(
        basePackages = "com.cjlabs.api",
        markerInterface = com.baomidou.mybatisplus.core.mapper.BaseMapper.class
        // annotationClass = Mapper.class  // 只扫描带 @Mapper 注解的接口
)
public class JobDelaySchedulerApplication {

    public static void main(String[] args) {
        ApplicationContextRunnerWrapper.run(JobDelaySchedulerApplication.class, args);
    }
}