package com.qc.itaojin.canalclient.kafka;

import com.qc.itaojin.canalclient.canal.entity.CanalOperationEntity;
import com.qc.itaojin.canalclient.canal.entity.ErrorEntity;
import com.qc.itaojin.canalclient.common.Constants;
import com.qc.itaojin.canalclient.enums.CanalOperationLevelEnum;
import com.qc.itaojin.canalclient.enums.CanalOperationTypeEnum;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.enums.ErrorTypeEnum;
import com.qc.itaojin.canalclient.kafka.counter.kafkaCounter;
import com.qc.itaojin.canalclient.mysql.service.IMysqlService;
import com.qc.itaojin.common.HBaseErrorCode;
import com.qc.itaojin.exception.ItaojinHBaseException;
import com.qc.itaojin.service.IHBaseService;
import com.qc.itaojin.service.IHiveService;
import com.qc.itaojin.util.JsonUtil;
import com.qc.itaojin.util.StringUtils;
import com.qc.itaojin.util.YamlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author fuqinqin
 * @desc kafka消费类
 * @date 2018-07-03
 */
@Component
@Slf4j
public class KafkaConsumer {

    @Autowired
    private IHiveService hiveService;
    @Autowired
    private IHBaseService hBaseService;
    @Autowired
    private IMysqlService mysqlService;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 物理数据库业务类型
     */
    private DataSourceTypeEnum ID;
    /**
     * 线程ID
     */
    private long threadId;

    /**
     * 记录错误重试的计数器
     */
    private kafkaCounter counter = new kafkaCounter();

    /**
     * 黑名单
     * */
    private Map<String, Object> blackMap = YamlUtil.load("black.yaml");

    @KafkaListener(topics = "itaojin_bigdata")
    public void processMessage(String content) {
        info("consumer message:{}", content);

        CanalOperationEntity operationEntity = null;
        String schema = null;
        String table = null;
        try {
            operationEntity = JsonUtil.parse(content, CanalOperationEntity.class);

            // DDL/DML
            CanalOperationLevelEnum operationLevelEnum = operationEntity.getOperationLevel();
            // 增删改
            CanalOperationTypeEnum operationTypeEnum = operationEntity.getOperationType();
            // schema
            schema = operationEntity.getSchema();
            // table
            table = operationEntity.getTable();
            ID = operationEntity.getID();
            threadId = operationEntity.getThreadId();

            // 过滤黑名单
            if(inBlackList(ID, schema, table)){
                return;
            }

            //DDL
            if (CanalOperationLevelEnum.TABLE.equalsTo(operationLevelEnum)) {
                // create table
                if (CanalOperationTypeEnum.CREATE.equalsTo(operationTypeEnum)) {
                    initSchema(schema, table);
                }
            }
            // DML
            else {
                // 命名空间
                String nameSpace = buildSchema(ID, schema);
                // HBase行键
                String rowKey = operationEntity.getRowKey();
                // 变化数据集
                Map<String, String> columnsMap = operationEntity.getColumnsMap();
                // 删除
                if (CanalOperationTypeEnum.DELETE.equalsTo(operationTypeEnum)) {
                    hBaseService.delete(nameSpace, table, rowKey);
                    info("删除一行数据成功！nameSpace={}, table={}, rowKey={}", nameSpace, table, rowKey);
                }
                // 添加/修改
                else {
                    hBaseService.update(nameSpace, table, rowKey, columnsMap);
                    if (CanalOperationTypeEnum.CREATE.equalsTo(operationTypeEnum)) {
                        info("新增数据成功！nameSpace={}, table={}, rowKey={}", nameSpace, table, rowKey);
                    } else {
                        info("修改数据成功！nameSpace={}, table={}, rowKey={}", nameSpace, table, rowKey);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

            // 判重
            if (operationEntity != null) {
                if (!counter.equalsBy(operationEntity.getBatchId())) {
                    counter.set(operationEntity.getBatchId());
                    counter.reset();
                } else {
                    counter.plus();
                }
            } else {
                counter.plus();
            }

            // 超过试错次数
            if (!counter.isUpperLimit(Constants.RETRY_NUMBER)) {
                log.error("kafka消费端异常次数未超上限，继续重试。。。");

                // HBase异常
                if(e instanceof ItaojinHBaseException){
                    ItaojinHBaseException hBaseException = (ItaojinHBaseException) e;
                    // table not exists
                    int errorCode = hBaseException.getErrorCode();
                    // 由于在初始化时由于网络延迟等原因漏掉一部分表的初始化，在这里重新初始化
                    if(errorCode == HBaseErrorCode.TABLE_NOT_FOUND){
                        log.info("表格{}:{}不存在，重建！", schema, table);
                        try {
                            initSchema(schema, table);
                        } catch (Exception e1) {
                            e1.printStackTrace();
                            counter.plus();
                        }
                    }
                }

                processMessage(content);
            } else {
                // 记录错误信息
                ErrorEntity errorEntity = new ErrorEntity(ErrorTypeEnum.KAFKA_CONSUME);
                errorEntity.setBizJson(content);
                errorEntity.setStackError(e.getMessage());
                kafkaTemplate.send(Constants.KafkaConstants.ERROR_TOPIC, JsonUtil.toJson(errorEntity));
            }
        }
    }

    /**
     * 判断是否在黑名单内
     * */
    private boolean inBlackList(DataSourceTypeEnum ID, String schema, String table) {
        if(MapUtils.isEmpty(blackMap)){
            return false;
        }

        for(Map.Entry<String, Object> entry : blackMap.entrySet()){
            String biz = entry.getKey();
            if(biz.equalsIgnoreCase(StringUtils.contact(ID.name(), ".", schema))){
                // table
                String t = (String) entry.getValue();
                if(table.equalsIgnoreCase(t)){
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 初始化Hive和HBase关联表
     * */
    private void initSchema(String schema, String table) throws Exception {
        // 生成HiveSQL
        String hiveSQL = mysqlService.generateHiveSQL(ID, schema, table);
        info("新建Hive和HBase关联表，HiveSQL={}", hiveSQL);
        if (StringUtils.isBlank(hiveSQL)) {
            return;
        }
        String hiveSchema = buildSchema(ID, schema);
        if (StringUtils.isBlank(hiveSchema) || !hiveService.init(hiveSchema, table, hiveSQL)) {
            info("初始化Hive仓库失败，buildHiveSchema={}, table={}, hiveSQL={}", hiveSchema, table, hiveSQL);
            // 此处记录异常，为人工维护提供数据支持
            throw new Exception(String.format("初始化Hive仓库失败，buildHiveSchema=%s, table=%s, hiveSQL=%s", hiveSchema, table, hiveSQL));
        }

        info("初始化Hive仓库成功！buildHiveSchema={}, table={}", hiveSchema, table);
    }

    /**
     * 构建hive仓库的schema
     */
    private String buildSchema(DataSourceTypeEnum dataSourceType, String schema) {
        if (dataSourceType == null || StringUtils.isBlank(schema)) {
            return null;
        }

        StringBuilder hiveSchema = new StringBuilder();
        hiveSchema.append(dataSourceType.name().toLowerCase())
                .append("000")
                .append(schema);

        return hiveSchema.toString();
    }

    /**
     * 多线程间个性输出日志
     */
    private void info(String content, Object... objects) {
        log.info(String.format(Constants.LOG_TEMPLATE, threadId, ID == null ? null : ID.name(), content), objects);
    }

    private void error(String content, Object... objects) {
        log.error(String.format(Constants.LOG_TEMPLATE, threadId, ID == null ? null : ID.name(), content), objects);
    }

    private void debug(String content, Object... objects) {
        log.debug(String.format(Constants.LOG_TEMPLATE, threadId, ID == null ? null : ID.name(), content), objects);
    }

    private void warn(String content, Object... objects) {
        log.warn(String.format(Constants.LOG_TEMPLATE, threadId, ID == null ? null : ID.name(), content), objects);
    }

}
