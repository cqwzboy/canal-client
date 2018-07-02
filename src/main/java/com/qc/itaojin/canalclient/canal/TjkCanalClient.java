package com.qc.itaojin.canalclient.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.qc.itaojin.canalclient.canal.entity.CanalOperationEntity;
import com.qc.itaojin.canalclient.canal.entity.CanalOperationEntity.Medium;
import com.qc.itaojin.canalclient.common.config.CanalConfiguration;
import com.qc.itaojin.canalclient.common.config.KafkaConfiguration;
import com.qc.itaojin.canalclient.common.config.ZookeeperConfiguration;
import com.qc.itaojin.canalclient.enums.CanalOperationLevelEnum;
import com.qc.itaojin.canalclient.enums.CanalOperationTypeEnum;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.enums.KeyTypeEnum;
import com.qc.itaojin.canalclient.mysql.service.IMysqlService;
import com.qc.itaojin.canalclient.util.BeanUtils;
import com.qc.itaojin.canalclient.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by fuqinqin on 2018/6/28.
 */
@Component
@Slf4j
public class TjkCanalClient extends Thread {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private CanalConfiguration canalConfig;
    @Autowired
    private KafkaConfiguration kafkaConfiguration;
    @Autowired
    private ZookeeperConfiguration zookeeperConfiguration;
    @Autowired
    private IMysqlService mysqlService;

    /**
     * canal客户端类型
     * */
    private DataSourceTypeEnum ID = DataSourceTypeEnum.TJK;

    /**
     * zk集群，多个以 , 隔开
     * */
    private String zkServers;
    /**
     *  canal instance
     * */
    private String destination;
    /**
     * 批量抓取数量
     * */
    private int batchSize;
    /**
     * 过滤正则
     * */
    private String filterRegex;
    /**
     * 客户端向服务端发送请求的频率
     * */
    private int requestInterval;

    @PostConstruct
    public void init(){
        zkServers = zookeeperConfiguration.getZkServers();
        batchSize = canalConfig.getBatchSize();
        destination = canalConfig.getDestination(ID);
        filterRegex = canalConfig.getFilterRegex(ID);
        requestInterval = canalConfig.getRequestInterval(ID);
    }

    @Override
    public void run() {
        // 创建链接（HA）
        CanalConnector connector = CanalConnectors.newClusterConnector(zkServers, destination, "", "");
        connector.connect();
        connector.subscribe(filterRegex);
        connector.rollback();
        while (true) {
            // 获取指定数量的数据
            Message message = connector.getWithoutAck(batchSize);
            long batchId = message.getId();
            int size = message.getEntries().size();
            boolean flag = true;
            if (batchId == -1 || size == 0) {
                log.info("tjk canal client listen server...");
                try {
                    Thread.sleep(requestInterval);
                } catch (InterruptedException e) {
                }
            } else {
                // 检查有效性
                if(!connector.checkValid()){
                    connector.disconnect();
                    connector = CanalConnectors.newClusterConnector(zkServers, destination, "", "");
                }

                // 执行
                try {
                    if(!process(message.getEntries())){
                        flag = false;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    flag = false;
                }
            }

            if(flag){
                // 提交确认
                connector.ack(batchId);
            } else {
                // 回滚
                connector.rollback();
            }
        }
    }

    /**
     * @desc 处理类
     * @param entrys 数据
     * */
    private boolean process(List<Entry> entrys) throws Exception {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                log.error("ERROR ## parser of eromanga-event has an error , data: {}", entry.toString());
                e.printStackTrace();
                return false;
            }

            EventType eventType = rowChage.getEventType();
            String schema = entry.getHeader().getSchemaName();
            String table = entry.getHeader().getTableName();
            String logfileName = entry.getHeader().getLogfileName();
            long logfileOffset = entry.getHeader().getLogfileOffset();
            log.info("================ binlog[{}:{}:{}] , name[{},{}] , eventType : {}",
                    logfileName,
                    logfileOffset,
                    entry.getHeader().getExecuteTime(),
                    schema,
                    table,
                    eventType);

            boolean isDdl = rowChage.getIsDdl();
            log.info("是否是ddl变更操作: {}", isDdl);
            log.info("具体的ddl sql: {}", rowChage.getSql());

            Medium medium = new Medium(ID);
            medium.setSchema(schema);
            medium.setTable(table);
            medium.setLogfileName(logfileName);
            medium.setLogfileOffset(logfileOffset);
            // 生成HBase的rowKey
            if (!setKeyType(schema, table, medium)) {
                continue;
            }


            // DDL操作
            if(isDdl){
                // TODO
                medium.setOperationLevel(CanalOperationLevelEnum.TABLE);
                if(EventType.CREATE == eventType){
                    medium.setOperationType(CanalOperationTypeEnum.CREATE);
                }
                CanalOperationEntity operationEntity = BeanUtils.copyProperties(medium, CanalOperationEntity.class);
                kafkaTemplate.send(kafkaConfiguration.getTopic(), JsonUtil.toJson(operationEntity));
            }
            // DML操作
            else{
                medium.setOperationLevel(CanalOperationLevelEnum.ROW);
                List<CanalOperationEntity> list = processRowData(eventType, rowChage.getRowDatasList(), medium);

                // 发布到kafka
                if(CollectionUtils.isNotEmpty(list)){
                    for (CanalOperationEntity operationEntity : list) {
                        kafkaTemplate.send(kafkaConfiguration.getTopic(), JsonUtil.toJson(operationEntity));
                    }
                }
            }
        }

        return true;
    }

    /**
     * @desc 查询表格对应的主键类型
     * @param schema 数据库实例
     * @param table 表
     * @param medium 中间介质
     * @return boolean true-成功 false-失败
     * */
    private boolean setKeyType(String schema, String table, Medium medium) {
        List<String> pks = mysqlService.getPKs(ID, schema, table);
        medium.setPks(pks);
        if(CollectionUtils.isEmpty(pks)){
            // TODO
            log.info("无主键数据更新，跳过");
            medium.setKeyType(KeyTypeEnum.NONE);
            return false;
        }else if(pks.size() == 1){
            medium.setKeyType(KeyTypeEnum.PRIMARY_KEY);
        }else{
            medium.setKeyType(KeyTypeEnum.COMBINE_KEY);
        }
        return true;
    }

    /**
     * @desc 数据库行操作对应的处理
     * @param rowDataList 一个批次的数据
     * @param eventType 增删改 类型
     * @param medium 中间介质
     * @return List<CanalOperationEntity> 封装类集合
     * */
    private List<CanalOperationEntity> processRowData(EventType eventType, List<RowData> rowDataList, Medium medium) throws Exception{
        List<CanalOperationEntity> reList = new ArrayList<>();

        if(CollectionUtils.isEmpty(rowDataList)){
            return reList;
        }

        List<Column> columns;

        for (RowData rowData : rowDataList) {
            CanalOperationEntity operationEntity = BeanUtils.copyProperties(medium, CanalOperationEntity.class);

            // 删除
            if (eventType == EventType.DELETE) {
                // 设置HBase的rowKey
                operationEntity.setRowKey(genRowKey(medium.getPks(), rowData.getBeforeColumnsList()));
                operationEntity.setOperationType(CanalOperationTypeEnum.DELETE);
            }
            // 增加
            else if (eventType == EventType.INSERT) {
                // 设置HBase的rowKey
                operationEntity.setRowKey(genRowKey(medium.getPks(), rowData.getAfterColumnsList()));

                operationEntity.setOperationType(CanalOperationTypeEnum.CREATE);

                columns = rowData.getAfterColumnsList();
                Map<String, String> columnsMap = new HashMap<>();
                for (Column column : columns) {
                    String columnName = column.getName();
                    String columnValue = column.getValue();
                    log.info("{} : {} : update={}", columnName, columnValue, column.getUpdated());
                    if(needAdd(medium.getPks(), medium.getKeyType(), columnName, CanalOperationTypeEnum.CREATE, true)){
                        columnsMap.put(columnName, columnValue);
                    }
                }
                operationEntity.setColumnsMap(columnsMap);
            }
            // 修改
            else {
                // 设置HBase的rowKey
                operationEntity.setRowKey(genRowKey(medium.getPks(), rowData.getAfterColumnsList()));

                operationEntity.setOperationType(CanalOperationTypeEnum.UPDATE);

                columns = rowData.getAfterColumnsList();
                Map<String, String> columnsMap = new HashMap<>();
                for (Column column : columns) {
                    String columnName = column.getName();
                    String columnValue = column.getValue();
                    boolean updated = column.getUpdated();
                    log.info("{} : {} : update={}", columnName, columnValue, updated);
                    if(needAdd(medium.getPks(), medium.getKeyType(), columnName, CanalOperationTypeEnum.UPDATE, updated)){
                        columnsMap.put(columnName, columnValue);
                    }
                }
                operationEntity.setColumnsMap(columnsMap);
            }

            reList.add(operationEntity);
        }

        return reList;
    }

    /**
     * 判断字段是否应该被添加
     * */
    private boolean needAdd(List<String> pks, KeyTypeEnum keyTypeEnum, String columnName, CanalOperationTypeEnum operationTypeEnum, Boolean updated) {
        // 该字段是主键
        if(pks.contains(columnName)){
            // 单主键，无论新增还是修改，都不应该添加
            if(KeyTypeEnum.PRIMARY_KEY.equalsTo(keyTypeEnum)){
                return false;
            }
            // 联合主键
            else if(KeyTypeEnum.COMBINE_KEY.equalsTo(keyTypeEnum)){
                // 新增情况应该添加
                if(CanalOperationTypeEnum.CREATE.equalsTo(operationTypeEnum)){
                    return true;
                }
                // 修改情况不应该被添加
                else {
                    return false;
                }
            }
        }
        // 非主键情况
        else{
            // 如果该字段被更新，应该被添加
            if(updated){
                return true;
            }else{
                return false;
            }
        }

        return false;
    }

    /**
     * @desc 生成HBase的rowKey
     * @param pks 主键集合
     * @param columns 列集合
     * @return String HBase的rowKey
     * */
    private String genRowKey(List<String> pks, List<Column> columns){
        String name;
        String value;
        StringBuilder sb = new StringBuilder();
        for (Column column : columns) {
            name = column.getName();
            value = column.getValue();
            if(pks.contains(name)){
                sb.append(value).append("_");
            }
        }

       sb.setLength(sb.length() -1);

        return sb.toString();
    }
}
