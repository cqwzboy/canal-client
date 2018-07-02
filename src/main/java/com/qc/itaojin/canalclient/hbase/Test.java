package com.qc.itaojin.canalclient.hbase;

import com.qc.itaojin.canalclient.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fuqinqin on 2018/7/2.
 */
public class Test {

    private static Configuration configuration;
    private static HBaseAdmin admin;

    static{
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "node4,node7,node8");
        configuration.set("hbase.master", "node2:600000");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
//        createTable("ns1:test");
//        insertData("tjk000test:t4");
//        deleteRow("tjk000test:t3", "23");
//        dropTable("tjk000test:t3");
//        queryAll("tjk000test:t4", "21", "23");
//        fuzzyQueryByRowkey("tjk000test:t4", "2");
    }

    /**
     * 创建表格
     * */
    public static void createTable(String tableName) throws IOException {

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表已存在，先删除...");
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));
        admin.createTable(tableDescriptor) ;
        admin.close();
        System.out.println("创建成功！");
    }

    /**
     * 插入数据
     * */
    public static void insertData(String tableName) throws IOException {
        System.out.println("start insert data..............");
        HTable hTable = new HTable(configuration, tableName);
        for (int i = 20; i < 25; i++) {
            Put put;
            put = new Put(String.valueOf(i).getBytes());
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
//            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(23));
//            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("address"), Bytes.toBytes("成都"));
            hTable.put(put);
        }
        hTable.close();

        System.out.println("插入成功");
    }

    /**
     * 删除一行数据
     * */
    public static void deleteRow(String tableName, String rowkey) throws IOException {
        HTable table = new HTable(configuration, tableName);
        List<Delete> list = new ArrayList<>();
        Delete delete = new Delete(rowkey.getBytes());
        list.add(delete);

        table.delete(list);
        System.out.println("删除行成功");
    }

    /**
     * 删除表
     * */
    public static void dropTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 查询全部
     * */
    public static void queryAll(String tableName, String startRow, String stopRow) throws IOException {
        HTable table = new HTable(configuration, tableName);
        Scan scan = new Scan () ;
        scan.addColumn("f1".getBytes(), "name".getBytes()) ;
        if(StringUtils.isNotBlank(startRow)){
            scan.setStartRow(startRow.getBytes());
        }
        if(StringUtils.isNotBlank(stopRow)){
            scan.setStopRow(stopRow.getBytes());
        }

        ResultScanner scanner = table.getScanner(scan) ;
        for(Result result : scanner) {
            System.out.print("rowKey:"+new String(result.getRow())+", ");
            for(KeyValue keyValue : result.raw()) {
                System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
            }
        }
    }

    /**
     * 模糊匹配rowkey
     * @param tableName
     * @param rowKeyRegex
     * @throws Exception
     */
    public static void fuzzyQueryByRowkey(String tableName, String rowKeyRegex) throws Exception
    {
        HTable table = new HTable(configuration, tableName) ;
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowKeyRegex)) ;
//      PrefixFilter filter = new PrefixFilter(rowKeyRegex.getBytes());
        Scan scan = new Scan();
        scan.addColumn("f1".getBytes(), "name".getBytes()) ;
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        int num=0;
        for(Result result : scanner)
        {
            num ++ ;

            System.out.println("rowKey:"+new String(result.getRow()));
            for(KeyValue keyValue : result.raw())
            {
                System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
            }
            System.out.println();
        }
        System.out.println(num);
    }
}
