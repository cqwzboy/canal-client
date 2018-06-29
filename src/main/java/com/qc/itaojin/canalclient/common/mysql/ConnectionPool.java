package com.qc.itaojin.canalclient.common.mysql;

import com.qc.itaojin.canalclient.common.config.MysqlConfiguration;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by fuqinqin on 2018/6/29.
 */
@Component
public class ConnectionPool {

    /**
     * String-物理数据库服务器类型
     * String-schema
     * Connection-连接
     * */
    private final Map<String, Map<String, Connection>> pools = new ConcurrentHashMap<>();

    @Autowired
    private MysqlConfiguration mysqlConfiguration;

    public Connection getConn(DataSourceTypeEnum dataSourceType, String schema){
        if(dataSourceType==null || StringUtils.isBlank(schema)){
            return null;
        }

        String sourceName = dataSourceType.name();

        if(pools.containsKey(sourceName)){
            if(pools.get(sourceName).containsKey(schema)){
                Connection conn = pools.get(sourceName).get(schema);
                // 校验连接的有效性
                if(!checkValid(conn)){
                    flush(dataSourceType, schema);
                }
            }else{
                flush(dataSourceType, schema);
            }
        }else{
            flush(dataSourceType, schema);
        }

        return pools.get(sourceName).get(schema);
    }

    private void flush(DataSourceTypeEnum dataSourceType, String schema){
        Connection connection = genConn(dataSourceType, schema);
        if(connection != null){
            if(!pools.containsKey(dataSourceType.name())){
                pools.put(dataSourceType.name(), new HashMap<>());
            }
            pools.get(dataSourceType.name()).put(schema, connection);
        }
    }

    /**
     * 生产数据库连接
     * */
    private Connection genConn(DataSourceTypeEnum dataSourceType, String schema){
        Connection conn = null;
        // 连接信息
        String driver = mysqlConfiguration.getDriver();
        String url = mysqlConfiguration.getUrl(dataSourceType);
        String userName = mysqlConfiguration.getUserName(dataSourceType);
        String password = mysqlConfiguration.getPassword(dataSourceType);

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return conn;
        }

        try {
            conn = DriverManager.getConnection(StringUtils.contact(url, schema),userName,password);
        } catch (SQLException e) {
            e.printStackTrace();
            return conn;
        }

        return conn;
    }

    private boolean checkValid(Connection connection){
        if(connection == null){
            return false;
        }

        PreparedStatement pstat = null;
        try {
            pstat = connection.prepareStatement("select 1 from dual");
            pstat.executeQuery();
        } catch (SQLException e) {
            return false;
        } finally {
            if(pstat != null){
                try {
                    pstat.close();
                } catch (SQLException e) {

                }
            }
        }

        return true;
    }

}
