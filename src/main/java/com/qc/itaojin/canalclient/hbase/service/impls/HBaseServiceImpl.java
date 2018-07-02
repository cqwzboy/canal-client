package com.qc.itaojin.canalclient.hbase.service.impls;

import com.qc.itaojin.canalclient.common.BaseService;
import com.qc.itaojin.canalclient.common.config.HBaseConfiguration;
import com.qc.itaojin.canalclient.hbase.service.IHBaseService;
import com.qc.itaojin.canalclient.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by fuqinqin on 2018/7/2.
 */
@Service
@Slf4j
public class HBaseServiceImpl extends BaseService implements IHBaseService {

    @Autowired
    private HBaseConfiguration hBaseConfiguration;

    private static final String DEFAULT_FAMILY = "f1";

    /**
     * 列族
     * */
    private String family = DEFAULT_FAMILY;

    @Override
    public boolean update(String nameSpace, String table, String rowKey, Map<String, String> columns) {
        if(StringUtils.isBlank(nameSpace) || StringUtils.isBlank(table) || MapUtils.isEmpty(columns)){
            log.info("【插入/修改 HBase表数据】参数不足， nameSpace={}, table={}, columns={}", nameSpace, table, columns.toString());
            return false;
        }

        String tableName = StringUtils.contact(nameSpace, ":", table);

        HTable hTable = null;
        try {
            hTable = new HTable(hBaseConfiguration.getConfiguration(), tableName);
            for(Map.Entry<String, String> entry : columns.entrySet()){
                Put put = new Put(toBytes(rowKey));
                String columnName = entry.getKey();
                String columnValue = entry.getValue();
                put.addColumn(toBytes(family), toBytes(columnName), toBytes(columnValue));
                hTable.put(put);
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(hTable != null){
                try {
                    hTable.close();
                } catch (IOException e) {

                }
            }
        }
        return false;
    }

    @Override
    public boolean delete(String nameSpace, String table, String rowKey) {
        String tableName = StringUtils.contact(nameSpace, ":", table);
        HTable hTable = null;
        try {
            hTable = new HTable(hBaseConfiguration.getConfiguration(), tableName);
            List<Delete> list = new ArrayList<>();
            Delete delete = new Delete(toBytes(rowKey));
            list.add(delete);

            hTable.delete(list);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(hTable != null){
                try {
                    hTable.close();
                } catch (IOException e) {

                }
            }
        }
        return false;
    }
}
