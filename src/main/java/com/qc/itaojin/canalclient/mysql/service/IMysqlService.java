package com.qc.itaojin.canalclient.mysql.service;

import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.enums.KeyTypeEnum;

import java.sql.ResultSet;
import java.util.List;

/**
 * Created by fuqinqin on 2018/7/2.
 */
public interface IMysqlService {

    /**
     * @desc 获取表格的主键（无主键，单主键，联合主键）
     * */
    public List<String> getPKs(DataSourceTypeEnum dataSourceType, String schema, String table);

    /**
     * 根据schema和table获取表格的主键类型
     * */
    public KeyTypeEnum analyseKeyType(DataSourceTypeEnum dataSourceType, String schema, String table);

    /**
     * 获取表内字段集合信息
     * */
    public ResultSet getColumnResultSet(DataSourceTypeEnum dataSourceType, String schema, String table);

    /**
     * @desc:根据MySQL中的schema和table生成HiveSQL
     * @param schema 数据库实例
     * @param table 表
     * */
    String generateHiveSQL(DataSourceTypeEnum dataSourceType, String schema, String table);

}
