package com.qc.itaojin.canalclient.common.dao;

import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.common.config.MysqlConfiguration;
import com.qc.itaojin.util.StringUtils;
import org.springframework.context.ApplicationContext;

import java.sql.*;

/**
 * Created by fuqinqin on 2018/6/25.
 */
public abstract class BaseDao {

    private String driver;
    private String url;
    private String userName;
    private String password;

    /**
     * 获取连接
     * */
    protected Connection getConn(){
        if(unInited()){
            MysqlConfiguration configuration =
                    ApplicationContextHolder.getBean("mysqlConfiguration",MysqlConfiguration.class);
            driver = configuration.getDriver();
            url = configuration.getBizUrl();
            userName = configuration.getBizUserName();
            password = configuration.getBizPassword();
        }
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, userName, password);
            return conn;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    protected int executeUpdate(String sql, Object[] params){
        Connection conn = null;
        PreparedStatement pstat = null;
        try{
            conn = getConn();
            pstat = conn.prepareStatement(sql);

            if(params!=null && params.length > 0){
                for (int i = 0; i < params.length; i++) {
                    pstat.setObject(i+1, params[i]);
                }
            }
            return pstat.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, pstat);
        }

        return -1;
    }

    private boolean unInited(){
        return StringUtils.isBlank(this.driver)
                && StringUtils.isBlank(this.url)
                && StringUtils.isBlank(this.userName)
                && StringUtils.isBlank(this.password);
    }

    protected void close(Connection conn){
        close(conn, null, null);
    }

    protected void close(Connection conn, Statement state){
        close(conn, state, null);
    }

    protected void close(Connection conn, Statement state, ResultSet res){
        close(state, conn, res);
    }

    protected void close(Statement state,Connection conn, ResultSet... res){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if(state != null){
            try {
                state.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if(res!=null && res.length>0){
            for (ResultSet re : res) {
                if(re != null){
                    try {
                        re.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
