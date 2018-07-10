package com.qc.itaojin.canalclient.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.qc.itaojin.annotation.PrototypeComponent;
import com.qc.itaojin.canalclient.canal.counter.CanalCounter;
import com.qc.itaojin.canalclient.canal.entity.CanalMessage;
import com.qc.itaojin.canalclient.canal.entity.CanalOperationEntity;
import com.qc.itaojin.canalclient.canal.entity.CanalOperationEntity.Medium;
import com.qc.itaojin.canalclient.canal.entity.ErrorEntity;
import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.common.Constants;
import com.qc.itaojin.canalclient.common.Constants.KafkaConstants;
import com.qc.itaojin.canalclient.common.config.CanalConfiguration;
import com.qc.itaojin.canalclient.enums.*;
import com.qc.itaojin.canalclient.mysql.service.IMysqlService;
import com.qc.itaojin.common.ZookeeperFactory;
import com.qc.itaojin.util.BeanUtils;
import com.qc.itaojin.util.JsonUtil;
import com.qc.itaojin.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by fuqinqin on 2018/6/28.
 */
@PrototypeComponent
@Slf4j
public class CanalClient extends Thread {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private CanalConfiguration canalConfig;
    @Autowired
    private IMysqlService mysqlService;

    @Value("${itaojin.zookeeper.quorum}")
    private String quorum;

    @Value("${itaojin.zookeeper.port}")
    private int port = 2181;

    /**
     * canal客户端类型
     * */
    private DataSourceTypeEnum ID;

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

    /**
     * 线程ID
     * */
    private long threadId;

    /**
     * 正常消费的topic
     * */
    private String normalTopic = KafkaConstants.NORMAL_TOPIC;

    /**
     * 异常信息消费topic
     * */
    private String errorTopic = KafkaConstants.ERROR_TOPIC;

    /**
     * 记录错误重试的计数器
     * */
    private CanalCounter counter = new CanalCounter();

    /**
     * 初始化身份 ID，并返回原对象
     * */
    public CanalClient init(DataSourceTypeEnum ID){
        this.ID = ID;
        return this;
    }

    private void initParams(){
        if(ID == null){
            throw new IllegalArgumentException("CanalClient's ID is null");
        }

        if(StringUtils.isBlank(quorum)){
            throw new IllegalArgumentException("quorum is null");
        }

        zkServers = ZookeeperFactory.ParamsParser.parseZKServers(quorum, port);
        batchSize = canalConfig.getBatchSize();
        destination = canalConfig.getDestination(ID);
        filterRegex = canalConfig.getFilterRegex(ID);
        requestInterval = canalConfig.getRequestInterval(ID);
        threadId = Thread.currentThread().getId();
    }

    @Override
    public void run() {
        // 初始化
        initParams();

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
                info("tjk canal client listen server...");
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
                    if(!process(message)){
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
     * @param message 数据
     * */
    private boolean process(Message message) throws Exception {
        int i = 0;
        for (Entry entry : message.getEntries()) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChange = null;
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());

                EventType eventType = rowChange.getEventType();
                String schema = entry.getHeader().getSchemaName();
                String table = entry.getHeader().getTableName();
                String logfileName = entry.getHeader().getLogfileName();
                long logfileOffset = entry.getHeader().getLogfileOffset();

                // 一批数据只在首条有效数据时判断
                if(i == 0){
                    if(!counter.equalsBy(logfileName, logfileOffset)){
                        counter.set(logfileName, logfileOffset);
                        counter.reset();
                    }else{
                        counter.plus();
                    }
                }
                i++;

                info("================ binlog[{}:{}:{}] , name[{},{}] , eventType : {}",
                        logfileName,
                        logfileOffset,
                        entry.getHeader().getExecuteTime(),
                        schema,
                        table,
                        eventType);

                boolean isDdl = rowChange.getIsDdl();
                info("是否是ddl变更操作: {}", isDdl);
                info("具体的ddl sql: {}", rowChange.getSql());

                Medium medium = new Medium(ID, threadId);
                medium.setBatchId(message.getId());
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
                    medium.setOperationLevel(CanalOperationLevelEnum.TABLE);
                    if(EventType.CREATE == eventType){
                        medium.setOperationType(CanalOperationTypeEnum.CREATE);
                    }
                    CanalOperationEntity operationEntity = BeanUtils.copyProperties(medium, CanalOperationEntity.class);
                    kafkaTemplate.send(normalTopic, JsonUtil.toJson(operationEntity));
                }
                // DML操作
                else{
                    medium.setOperationLevel(CanalOperationLevelEnum.ROW);
                    List<CanalOperationEntity> list = processRowData(eventType, rowChange.getRowDatasList(), medium);

                    // 发布到kafka
                    if(CollectionUtils.isNotEmpty(list)){
                        for (CanalOperationEntity operationEntity : list) {
                            kafkaTemplate.send(normalTopic, JsonUtil.toJson(operationEntity));
                        }
                    }
                }
            } catch (Exception e) {
                error("ERROR ## parser of eromanga-event has an error , data: {}, begin retry, error info would be recorded after {} retry times",
                        entry.toString(), Constants.RETRY_NUMBER);
                e.printStackTrace();
                // 超过试错次数
                if(counter.isUpperLimit(Constants.RETRY_NUMBER)){
                    log.error("canal客户端异常次数已超上限");
                    // 让canal client 提交ack确认，系统记录错误，客户端继续消费后面的消息
                    ErrorEntity errorEntity = new ErrorEntity(ErrorTypeEnum.CANAL_LISTEN);
                    errorEntity.setBizJson(JsonUtil.toJson(CanalMessage.build(message)));
                    errorEntity.setStackError(e.getMessage());
                    kafkaTemplate.send(errorTopic, JsonUtil.toJson(errorEntity));
                    return true;
                }
                return false;
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
            info("无主键数据更新，跳过");
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
        try{
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
                        info("{} : {} : update={}", columnName, columnValue, column.getUpdated());
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
                        info("{} : {} : update={}", columnName, columnValue, updated);
                        if(needAdd(medium.getPks(), medium.getKeyType(), columnName, CanalOperationTypeEnum.UPDATE, updated)){
                            columnsMap.put(columnName, columnValue);
                        }
                    }
                    operationEntity.setColumnsMap(columnsMap);
                }

                reList.add(operationEntity);
            }

            return reList;
        }catch (Exception e){
            throw new Exception(e);
        }
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

    /**
     * 多线程间个性输出日志
     * */
    private void info(String content, Object... objects){
        log.info(String.format(Constants.LOG_TEMPLATE, threadId, ID.name(), content), objects);
    }
    private void error(String content, Object... objects){
        log.error(String.format(Constants.LOG_TEMPLATE, threadId, ID.name(), content), objects);
    }
    private void debug(String content, Object... objects){
        log.debug(String.format(Constants.LOG_TEMPLATE, threadId, ID.name(), content), objects);
    }
    private void warn(String content, Object... objects){
        log.warn(String.format(Constants.LOG_TEMPLATE, threadId, ID.name(), content), objects);
    }

    /**
     * 工厂类
     * */
    public static class CanalClientFactory{
        public static CanalClient get(DataSourceTypeEnum dataSourceType){
            Assert.notNull(dataSourceType, "dataSourceType must not be null");
            return ApplicationContextHolder.getBean("canalClient", CanalClient.class).init(dataSourceType);
        }

        public static List<CanalClient> get(List<DataSourceTypeEnum> dataSourceTypeList){
            Assert.notEmpty(dataSourceTypeList, "dataSourceTypeList must not be empty");

            List<CanalClient> _list = new ArrayList<>();
            for (DataSourceTypeEnum dataSourceTypeEnum : dataSourceTypeList) {
                _list.add(get(dataSourceTypeEnum));
            }

            return _list;
        }
    }
}
