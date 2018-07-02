package com.qc.itaojin.canalclient.hive.service;

import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;

import java.util.Map;

/**
 * Created by fuqinqin on 2018/6/29.
 */
public interface IHiveGenericService {

    /**
     * @desc:根据MySQL中的schema和table生成HiveSQL
     * @param schema 数据库实例
     * @param table 表
     * */
    String generateHiveSQL(DataSourceTypeEnum dataSourceType, String schema, String table);

    /**
     * 初始化Hive
     * */
    boolean initBySchema(String schema, String table, String sql);

}
