package com.qc.itaojin.canalclient.common.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by fuqinqin on 2018/6/29.
 */
@Component
@ConfigurationProperties(prefix = "kafka")
@PropertySource(value = "classpath:local.yaml")
@Data
public class KafkaConfiguration {

    @Value("${topic}")
    private String topic;

}
