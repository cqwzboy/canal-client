package com.qc.itaojin.canalclient.common.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by fuqinqin on 2018/6/28.
 */
@Component
@ConfigurationProperties(prefix = "canal")
@PropertySource(value = "classpath:local.yaml")
@Data
public class CanalConfig {

    @Value("${zk-servers}")
    private String zkServers;

    @Value("${destination}")
    private String destination;

    @Value("${batch-size}")
    private int batchSize;

    @Value("${filter-regex}")
    private String filterRegex;

    @Value("${request-interval}")
    private int requestInterval;

}
