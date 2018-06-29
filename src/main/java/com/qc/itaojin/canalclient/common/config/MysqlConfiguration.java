package com.qc.itaojin.canalclient.common.config;

import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by fuqinqin on 2018/6/29.
 */
@Component
@ConfigurationProperties(prefix = "mysql")
@PropertySource(value = "classpath:local.yaml")
@Data
public class MysqlConfiguration extends BaseConfiguration{

    /**
     * 驱动
     * */
    @Value("${driver}")
    private String driver;

    /******************************** TJK ********************************/
    @Value("${tjk-url}")
    private String tjkUrl;
    @Value("${tjk-user-name}")
    private String tjkUserName;
    @Value("${tjk-password}")
    private String tjkPassword;

    /******************************** AI ********************************/
    @Value("${ai-url}")
    private String aiUrl;
    @Value("${ai-user-name}")
    private String aiUserName;
    @Value("${ai-password}")
    private String aiPassword;

    /******************************** 支付 ********************************/
    @Value("${pay-url}")
    private String payUrl;
    @Value("${pay-user-name}")
    private String payUserName;
    @Value("${pay-password}")
    private String payPassword;

    /******************************** 工作台 ********************************/
    @Value("${bench-url}")
    private String benchUrl;
    @Value("${bench-user-name}")
    private String benchUserName;
    @Value("${bench-password}")
    private String benchPassword;

    public String getUrl(DataSourceTypeEnum dataSourceTyp){
        return invoke(dataSourceTyp, "Url");
    }

    public String getUserName(DataSourceTypeEnum dataSourceTyp){
        return invoke(dataSourceTyp, "UserName");
    }

    public String getPassword(DataSourceTypeEnum dataSourceTyp){
        return invoke(dataSourceTyp, "Password");
    }

}
