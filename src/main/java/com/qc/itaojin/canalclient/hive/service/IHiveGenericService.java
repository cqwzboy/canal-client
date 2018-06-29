package com.qc.itaojin.canalclient.hive.service;

/**
 * Created by fuqinqin on 2018/6/29.
 */
public interface IHiveGenericService {

    /**
     * @desc:根据MySQL中的schema和table生成HiveSQL
     * @param schema 数据库实例
     * @param table 表
     * */
    String generateHiveSQL(String schema, String table);

}
