package com.qc.itaojin.canalclient.common.config;

import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.util.StringUtils;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by fuqinqin on 2018/6/28.
 */
@Component
@ConfigurationProperties(prefix = "canal")
@PropertySource(value = "classpath:local.yaml")
@Data
public class CanalConfiguration extends BaseConfiguration{

    @Value("${zk-servers}")
    private String zkServers;
    @Value("${batch-size}")
    private int batchSize;

    /****************************** tjk ***********************************/
    @Value("${tjk-destination}")
    private String tjkDestination;
    @Value("${tjk-filter-regex}")
    private String tjkFilterRegex;
    @Value("${tjk-request-interval}")
    private int tjkRequestInterval;

    /****************************** ai ***********************************/
    @Value("${ai-destination}")
    private String aiDestination;
    @Value("${ai-filter-regex}")
    private String aiFilterRegex;
    @Value("${ai-request-interval}")
    private int aiRequestInterval;

    /****************************** 支付 ***********************************/
    @Value("${pay-destination}")
    private String payDestination;
    @Value("${pay-filter-regex}")
    private String payFilterRegex;
    @Value("${pay-request-interval}")
    private int payRequestInterval;

    /****************************** 工作台 ***********************************/
    @Value("${bench-destination}")
    private String benchDestination;
    @Value("${bench-filter-regex}")
    private String benchFilterRegex;
    @Value("${bench-request-interval}")
    private int benchRequestInterval;

    public String getDestination(DataSourceTypeEnum dataSourceType){
        return invoke(dataSourceType, "Destination");
    }

    public String getFilterRegex(DataSourceTypeEnum dataSourceType){
        return invoke(dataSourceType, "FilterRegex");
    }

    public int getRequestInterval(DataSourceTypeEnum dataSourceType){
        return invoke(dataSourceType, "RequestInterval");
    }

}
