package com.qc.itaojin.canalclient.common.config;

import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by fuqinqin on 2018/6/28.
 */
@Component
@ConfigurationProperties(prefix = "itaojin.canal")
@Data
public class CanalConfiguration extends BaseConfiguration{
    private int batchSize;

    /****************************** tjk ***********************************/
    private String tjkDestination;
    private String tjkFilterRegex;
    private int tjkRequestInterval;

    /****************************** ai ***********************************/
    private String aiDestination;
    private String aiFilterRegex;
    private int aiRequestInterval;

    /****************************** 支付 ***********************************/
    private String payDestination;
    private String payFilterRegex;
    private int payRequestInterval;

    /****************************** 工作台 ***********************************/
    private String benchDestination;
    private String benchFilterRegex;
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
