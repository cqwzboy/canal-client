package com.qc.itaojin.canalclient.canal.entity;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @desc canal的Message实体类再封装
 * @author fuqinqin
 * @date 2018-07-09
 */
@Data
public class CanalMessage {

    /**
     * 批次ID
     * */
    private long batchId;

    /**
     * 数据集合大小
     * */
    private int size;

    /**
     * 集合
     * */
    private List<Entry> entryList;

    /**
     * Entry
     * */
    @Data
    public static class Entry{
        private Header header;
        /**
         * 事务头BEGIN/事务尾END/数据ROWDATA
         * */
        private String entryType;
        /**
         * 是否是ddl变更操作，比如create table/drop table
         * */
        private boolean isDdl;
        /**
         * 具体的ddl sql
         * */
        private String sql;
        /**
         * 行数据集合
         * */
        private List<RowData> rowDatas;
    }

    /**
     * RowData
     * */
    @Data
    public static class RowData{
        private List<Column> beforeColumns;
        private List<Column> afterColumns;
    }

    /**
     * Column
     * */
    @Data
    public static class Column{
        /**
         * column序号
         * */
        private int index;
        /**
         * jdbc type
         * */
        private int sqlType;
        /**
         * column name
         * */
        private String name;
        /**
         * 是否为主键
         * */
        private boolean isKey;
        /**
         * 是否发生过变更
         * */
        private boolean updated;
        /**
         * 值是否为null
         * */
        private boolean isNull;
        /**
         * 值
         * */
        private String value;

        public Column(CanalEntry.Column column){
            this.index = column.getIndex();
            this.sqlType = column.getSqlType();
            this.name = column.getName();
            this.isKey = column.getIsKey();
            this.updated = column.getUpdated();
            this.isNull = column.getIsNull();
            this.value = column.getValue();
        }
    }

    /**
     * Header
     * */
    @Data
    public static class Header{
        /**
         * binlog文件名
         * */
        private String logfileName;
        /**
         * binlog position
         * */
        private long logfileOffset;
        /**
         * binlog里记录变更发生的时间戳
         * */
        private long executeTime;
        /**
         * 数据库实例
         * */
        private String schemaName;
        /**
         * 表名
         * */
        private String tableName;
        /**
         * 操作类型
         * */
        private String eventType;

        public Header(CanalEntry.Header header){
            this.logfileName = header.getLogfileName();
            this.logfileOffset = header.getLogfileOffset();
            this.executeTime = header.getExecuteTime();
            this.schemaName = header.getSchemaName();
            this.tableName = header.getTableName();
            this.eventType = header.getEventType().name();
        }
    }

    public static CanalMessage build(Message message){
        if(message==null){
            return null;
        }

        CanalMessage canalMessage = new CanalMessage();
        canalMessage.setBatchId(message.getId());
        canalMessage.setSize(message.getEntries().size());

        List<CanalEntry.Entry> entries = message.getEntries();
        List<Entry> entryList = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(entries)){
            for (CanalEntry.Entry e : entries) {
                Entry entry = new Entry();
                entry.setEntryType(e.getEntryType().name());
                ByteString storeValue = e.getStoreValue();

                // step 1.
                entry.setHeader(new Header(e.getHeader()));

                // step 2.
                try {
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                    entry.setDdl(rowChange.getIsDdl());
                    entry.setSql(rowChange.getSql());
                    List<CanalEntry.RowData> rowDatas = rowChange.getRowDatasList();
                    if(CollectionUtils.isNotEmpty(rowDatas)){
                        List<RowData> rowDataList = new ArrayList<>();
                        for (CanalEntry.RowData r : rowDatas) {
                            RowData rowData = new RowData();
                            rowData.setBeforeColumns(parseColumn(r.getBeforeColumnsList()));
                            rowData.setAfterColumns(parseColumn(r.getAfterColumnsList()));
                            rowDataList.add(rowData);
                        }
                        entry.setRowDatas(rowDataList);
                    }
                } catch (InvalidProtocolBufferException e1) {
                    e1.printStackTrace();
                }

                entryList.add(entry);
            }
        }

        canalMessage.setEntryList(entryList);

        return canalMessage;
    }

    private static List<Column> parseColumn(List<CanalEntry.Column> columnList){
        if(CollectionUtils.isEmpty(columnList)){
            return null;
        }

        List<Column> list = new ArrayList<>();

        for (CanalEntry.Column c : columnList) {
            Column column = new Column(c);
            list.add(column);
        }

        return list;
    }

}
