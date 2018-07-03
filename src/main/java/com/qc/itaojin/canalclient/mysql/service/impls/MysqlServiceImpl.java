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

import static com.qc.itaojin.canalclient.enums.MysqlDataTypeEnum.nameOf;
import static com.qc.itaojin.canalclient.enums.MysqlDataTypeEnum.transToHiveDataType;

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

    @Override
    public String generateHiveSQL(DataSourceTypeEnum dataSourceType, String schema, String table) {
        // 获取MySQL表的主键集合
        List<String> pks = this.getPKs(dataSourceType, schema, table);
        // 获取MySQL表格的主键类型
        KeyTypeEnum keyType = this.analyseKeyType(dataSourceType, schema, table);

        // 获取表内字段名称和类型
        ResultSet resultSet = null;
        resultSet = this.getColumnResultSet(dataSourceType, schema, table);
        String columnName;
        String columnType;
        String comment;
        // 拼接Hive的sql缓存区
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ")
                .append(table).append("\n")
                .append("(").append("\n");
        //映射字符串
        StringBuilder mapStr = new StringBuilder(":key,");
        //如果表是联合主键，则Hive的建表语句中需要新增一列：联合主键
        if(keyType.equalsTo(KeyTypeEnum.COMBINE_KEY)){
            sql.append("COMBINE_KEY_121414125").append(" ").append("varchar(4000)").append(" ").append("comment '联合主键',\n");
        }
        try{
            // 列数，如果在单主键的表格中只有一个列，则需要添加一列占位列，否则hbase报错
            int columnNum = 0;
            while (resultSet.next()){
                columnName = resultSet.getString("COLUMN_NAME");
                columnType = resultSet.getString("TYPE_NAME");
                comment = resultSet.getString("REMARKS");

                columnNum++;

                //注释中存在分号则替换成逗号
                comment = comment.replaceAll(";", ",");

                // 字段定义
                sql.append("`").append(columnName).append("` ")
                        .append(transToHiveDataType(nameOf(columnType))).append(" ")
                        .append("comment '").append(comment).append("',").append("\n");

                // 当且仅当是单主键时":key"对应的是主键，故而无需再次申明
                if(keyType.equalsTo(KeyTypeEnum.PRIMARY_KEY) && pks.contains(columnName)){
                    continue;
                }
                mapStr.append("f1:").append(columnName).append(",");
            }

            if(keyType.equalsTo(KeyTypeEnum.PRIMARY_KEY) && columnNum==1){
                sql.append("`placeholder` int comment 'placeholder',\n");
                mapStr.append("f1:placeholder,");
            }

            sql.setLength(sql.length() - 2);
            mapStr.setLength(mapStr.length() - 1);

            sql.append("\n")
                    .append(")").append("\n")
                    .append("ROW FORMAT").append("\n")
                    .append("SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'").append("\n")
                    .append("STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'").append("\n")
                    .append("WITH SERDEPROPERTIES (").append("\n")
                    .append("'serialization.format'='\\t',").append("\n")
                    .append("'hbase.columns.mapping'='").append(mapStr.toString()).append("',").append("\n")
                    .append("'field.delim'='\\t'").append("\n")
                    .append(")").append("\n")
                    .append("TBLPROPERTIES ('hbase.table.name'='"+buildHBaseNameSpace(dataSourceType, schema)+":").append(table).append("')");
        }catch (SQLException e){
            e.printStackTrace();
        }

        return sql.toString();
    }

    /**
     * 构建HBase的namespace，由于HBase的namespace的构成只允许字母和数字，所以这里使用“000”作为物理库和schema的分隔符
     * */
    private String buildHBaseNameSpace(DataSourceTypeEnum dataSourceType, String schema){
        StringBuilder nameSpace = new StringBuilder();
        nameSpace.append(dataSourceType.name().toLowerCase())
                .append("000")
                .append(schema);

        return nameSpace.toString();
    }
}
