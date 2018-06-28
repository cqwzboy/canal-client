package com.qc.itaojin.canalclient.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.qc.itaojin.canalclient.common.config.CanalConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by fuqinqin on 2018/5/28.
 */
@Component
@Slf4j
public class CanalClient extends Thread {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private CanalConfig canalConfig;

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
        zkServers = canalConfig.getZkServers();
        destination = canalConfig.getDestination();
        batchSize = canalConfig.getBatchSize();
        filterRegex = canalConfig.getFilterRegex();
        requestInterval = canalConfig.getRequestInterval();
    }

    @Override
    public void run() {
        // 创建链接（HA）
        CanalConnector connector = CanalConnectors.newClusterConnector(zkServers, destination, "", "");
        try {
            connector.connect();
            connector.subscribe(filterRegex);
            connector.rollback();
            while (true) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    log.info("canal client listen server...");
                    try {
                        Thread.sleep(requestInterval);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // 检查有效性
                    if(!connector.checkValid()){
                        connector = CanalConnectors.newClusterConnector(zkServers, destination, "", "");
                    }
                    process(message.getEntries());
                }

                // 提交确认
                connector.ack(batchId);
                // 处理失败, 回滚数据
//                connector.rollback(batchId);

            }
        } finally {
            connector.disconnect();
        }
    }

    private static void process(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(), entry.getHeader().getExecuteTime(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            System.out.println("是否是ddl变更操作: "+rowChage.getIsDdl());
            System.out.println("具体的ddl sql: "+rowChage.getSql());

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("-------&gt; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------&gt; after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}
