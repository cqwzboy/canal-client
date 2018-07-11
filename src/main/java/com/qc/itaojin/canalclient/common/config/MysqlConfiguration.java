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
@Data
public class MysqlConfiguration extends BaseConfiguration{

    /**
     * 驱动
     * */
    private String driver;

    /******************************** TJK ********************************/
    private String tjkUrl;
    private String tjkUserName;
    private String tjkPassword;

    /******************************** AI ********************************/
    private String aiUrl;
    private String aiUserName;
    private String aiPassword;

    /******************************** 支付 ********************************/
    private String payUrl;
    private String payUserName;
    private String payPassword;

    /******************************** 工作台 ********************************/
    private String benchUrl;
    private String benchUserName;
    private String benchPassword;

    /******************************** 系统自用数据库，必有！ ********************************/
    private String bizUrl;
    private String bizUserName;
    private String bizPassword;

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
