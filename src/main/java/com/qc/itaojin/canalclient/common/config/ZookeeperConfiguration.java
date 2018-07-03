package com.qc.itaojin.canalclient.common.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by fuqinqin on 2018/7/2.
 */
@Component
@ConfigurationProperties(prefix = "zookeeper")
@PropertySource(value = "classpath:local.yaml")
@Data
public class ZookeeperConfiguration {

    @Value("${servers}")
    private String zkServers;

}
