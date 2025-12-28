package com.cjlabs.api.config.job;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "xxl.job")
public class XxlJobProperties {

    private Admin admin = new Admin();
    private Executor executor = new Executor();

    @Data
    public static class Admin {
        private String addresses;
        private String accessToken;
        private int timeout;
        private String username;
        private String password;
    }

    @Data
    public static class Executor {
        private boolean enabled;
        private String appname;
        private String address;
        private String ip;
        private int port;
        private String logpath;
        private int logretentiondays;
        private String excludedpackage;
    }
}