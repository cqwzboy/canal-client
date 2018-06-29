package com.qc.itaojin.canalclient.common.mysql;

import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.enums.KeyTypeEnum;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @desc Mysql帮助类，可以实现一些Mysql相关的操作
 * @author fuqinqin
 * @date 2018-06-29
 */
@Component
public class MysqlHelper {

    @Autowired
    private ConnectionPool connectionPool;

    /**
     * 获取表格的主键（无主键，单主键，联合主键）
     * */
    public List<String> getPKs(DataSourceTypeEnum dataSourceType, String schema, String table){
        if(dataSourceType==null || StringUtils.isBlank(schema) || StringUtils.isBlank(table)){
            return null;
        }

        Connection connection = null;
        ResultSet pkResultSet = null;

        try {
            // 从连接池中获取连接
            connection = connectionPool.getConn(dataSourceType, schema);
            if(connection == null){
                return null;
            }

            // 获取元数据独享
            DatabaseMetaData metaData = connection.getMetaData();
            List<String> pks = new ArrayList<>();

            // 查询主键
            pkResultSet = metaData.getPrimaryKeys(null, schema, table);
            while(pkResultSet.next()){
                pks.add(pkResultSet.getString("COLUMN_NAME"));
            }

            return pks;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(pkResultSet != null){
                try {
                    pkResultSet.close();
                } catch (SQLException e) {
                }
            }
        }

        return null;
    }

    /**
     * 根据schema和table获取表格的主键类型
     * */
    public KeyTypeEnum analyseKeyType(DataSourceTypeEnum dataSourceType, String schema, String table){
        List<String> pks = getPKs(dataSourceType, schema, table);

        if(CollectionUtils.isEmpty(pks)){
            return KeyTypeEnum.NONE;
        }else if(pks.size() == 1) {
            return KeyTypeEnum.PRIMARY_KEY;
        }else{
            return KeyTypeEnum.COMBINE_KEY;
        }
    }

}
