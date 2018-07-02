package com.qc.itaojin.canalclient.hive.dao;

import com.qc.itaojin.canalclient.annotation.PrototypeComponent;
import com.qc.itaojin.canalclient.common.config.HiveConfiguration;
import com.qc.itaojin.canalclient.common.dao.BaseDao;
import com.qc.itaojin.canalclient.util.StringUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.sql.*;
import java.util.*;

/**
 * Created by fuqinqin on 2018/6/25.
 */
@PrototypeComponent
public class HiveBaseDao extends BaseDao {

    @Autowired
    private HiveConfiguration hiveConfiguration;

    private String driver;
    private String url;

    protected String schema;

    @PostConstruct
    public void init(){
        this.driver = hiveConfiguration.getDriver();
        this.url = hiveConfiguration.getUrl();
    }

    public void setSchema(String schema){
        this.schema = schema;
    }

    @Override
    protected String getSchema() {
        return this.schema;
    }

    /**
     * 获取Hive的连接对象
     * */
    public Connection getConn(){
        Connection conn = null;

        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(StringUtils.contact(url, getSchema()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * 执行Hive的SQL
     * */
    public boolean execute(String sql){
        if(StringUtils.isBlank(sql)){
            return false;
        }

        Connection conn = null;
        PreparedStatement pstat = null;

        try{
            conn = getConn();
            if(conn == null){
                return false;
            }

            pstat = conn.prepareStatement(sql);
            pstat.execute();

            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, pstat);
        }

        return false;
    }

    /**
     * 获取Hive中所有数据库实例集合
     * */
    public Set<String> findAllDataBases(){
        Connection conn = null;
        PreparedStatement pstat = null;
        ResultSet res = null;

        try {
            Set<String> set = new HashSet<>();
            conn = getConn();
            pstat = conn.prepareStatement("show databases");
            res = pstat.executeQuery();
            if(res != null){
                while (res.next()){
                    set.add(res.getString(1));
                }
            }

            return set;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, pstat, res);
        }

        return null;
    }

    /**
     * 获取Hive中所有数据库实例集合
     * */
    public Set<String> findAllTables(){
        Connection conn = null;
        PreparedStatement pstat = null;
        ResultSet res = null;

        try {
            Set<String> set = new HashSet<>();
            conn = getConn();
            pstat = conn.prepareStatement("show tables in "+getSchema());
            res = pstat.executeQuery();
            if(res != null){
                while (res.next()){
                    set.add(res.getString(1));
                }
            }

            return set;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, pstat, res);
        }

        return null;
    }

    /**
     * 查询某个数据库实例是否已经存在
     * */
    public boolean schemaExists(String schema){
        if(StringUtils.isBlank(schema)){
            return false;
        }

        Set<String> schemas = findAllDataBases();
        if(CollectionUtils.isEmpty(schemas)){
            return false;
        }

        return schemas.contains(schema);
    }

}
