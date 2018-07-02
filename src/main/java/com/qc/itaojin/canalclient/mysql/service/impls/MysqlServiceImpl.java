package com.qc.itaojin.canalclient.mysql.service.impls;

import com.qc.itaojin.canalclient.common.mysql.ConnectionPool;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.enums.KeyTypeEnum;
import com.qc.itaojin.canalclient.mysql.service.IMysqlService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fuqinqin on 2018/7/2.
 */
@Service
public class MysqlServiceImpl implements IMysqlService {

    @Autowired
    private ConnectionPool connectionPool;

    /**
     * 获取表格的主键（无主键，单主键，联合主键）
     * */
    @Override
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
    @Override
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

    /**
     * 获取表内字段集合信息
     * */
    @Override
    public ResultSet getColumnResultSet(DataSourceTypeEnum dataSourceType, String schema, String table) {
        Connection connection = null;
        ResultSet resultSet = null;

        try {
            connection = connectionPool.getConn(dataSourceType, schema);
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getColumns(null, schema, table, "%");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;
    }
}
