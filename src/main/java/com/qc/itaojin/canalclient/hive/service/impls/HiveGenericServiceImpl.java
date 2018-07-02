package com.qc.itaojin.canalclient.hive.service.impls;

import com.qc.itaojin.canalclient.annotation.PrototypeService;
import com.qc.itaojin.canalclient.common.Constants.HiveConstants;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.enums.KeyTypeEnum;
import com.qc.itaojin.canalclient.hive.dao.HiveBaseDao;
import com.qc.itaojin.canalclient.hive.service.IHiveGenericService;
import com.qc.itaojin.canalclient.mysql.service.IMysqlService;
import com.qc.itaojin.canalclient.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static com.qc.itaojin.canalclient.enums.MysqlDataTypeEnum.nameOf;
import static com.qc.itaojin.canalclient.enums.MysqlDataTypeEnum.transToHiveDataType;

/**
 * @desc Hive服务类
 * @author fuqinqin
 * @date 2018-07-02
 */
@PrototypeService
@Slf4j
public class HiveGenericServiceImpl implements IHiveGenericService {

    @Autowired
    private IMysqlService mysqlService;
    @Autowired
    private HiveBaseDao hiveBaseDao;

    @Override
    public String generateHiveSQL(DataSourceTypeEnum dataSourceType, String schema, String table) {
        // 获取MySQL表的主键集合
        List<String> pks = mysqlService.getPKs(dataSourceType, schema, table);
        // 获取MySQL表格的主键类型
        KeyTypeEnum keyType = mysqlService.analyseKeyType(dataSourceType, schema, table);

        // 获取表内字段名称和类型
        ResultSet resultSet = null;
        resultSet = mysqlService.getColumnResultSet(dataSourceType, schema, table);
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

    @Override
    public boolean initBySchema(String schema, String table, String sql) {
        if(StringUtils.isBlank(schema) || StringUtils.isBlank(table) || StringUtils.isBlank(sql)){
            return false;
        }

        // step 1. 查询该schema是否存在，不存在则新建
        hiveBaseDao.setSchema(HiveConstants.DEFAULT_SCHEMA);
        if(!hiveBaseDao.schemaExists(schema)){
            log.info("Hive 不存在实例{}，新建之", schema);

            // step 2. 创建schema
            if(!hiveBaseDao.execute("create database "+schema)){
                log.info("Hive 创建数据库实例{} 失败", schema);
                return false;
            }
        }

        // step 3. 判断新建的表格在Hive中是否已经存在，若不存在则新建
        hiveBaseDao.setSchema(schema);
        Set<String> oldTables = hiveBaseDao.findAllTables();
        if(oldTables.contains(table)){
            log.info("schema={}中已经存在table={}", schema, table);
            return true;
        }

        // step 4. 建表
        if(!hiveBaseDao.execute(sql)){
            log.info("Hive 建表 {}.{} 失败", schema, table);
            return false;
        }

        log.info("初始化Hive数据库 {}.{} 成功！", schema, table);

        return true;
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
