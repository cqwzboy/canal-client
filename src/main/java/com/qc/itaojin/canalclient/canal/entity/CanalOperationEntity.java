package com.qc.itaojin.canalclient.canal.entity;

import com.qc.itaojin.canalclient.enums.CanalOperationLevelEnum;
import com.qc.itaojin.canalclient.enums.CanalOperationTypeEnum;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.enums.KeyTypeEnum;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @desc Canal数据的封装类
 * @author fuqinqin
 * @date 2018-06-28
 */
@Data
public class CanalOperationEntity {

    /**
     * HBase Row Key
     * */
    private String rowKey;

    /**
     * 物理MySQL数据库类型（业务分类）
     * */
    private DataSourceTypeEnum ID;

    /**
     * 线程id
     * */
    private long threadId;

    /**
     * 库
     * */
    private String schema;

    /**
     * 表
     * */
    private String table;

    /**
     * 操作类型
     * */
    private CanalOperationTypeEnum operationType;

    /**
     * 操作级别
     * */
    private CanalOperationLevelEnum operationLevel;

    /**
     * 相关操作对应的字段和值值键值map
     * */
    private Map<String, String> columnsMap;

    /**
     * 若是增加表格操作，则会生成一个HiveSQL的字段
     * */
    private String hiveSql;

    /**
     * binlog文件名
     * */
    private String logfileName;

    /**
     * binlog position
     * */
    private long logfileOffset;

    @Data
    public static class Medium extends CanalOperationEntity{

        public Medium(DataSourceTypeEnum ID, long threadId){
            super.setID(ID);
            super.setThreadId(threadId);
        }

        /**
         * MySQL数据库表的主键类型
         * */
        private KeyTypeEnum keyType;

        /**
         * 主键列表
         * */
        private List<String> pks;
    }

}
