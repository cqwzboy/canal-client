package com.qc.itaojin.canalclient.test;

import com.qc.itaojin.annotation.HBaseColumn;
import com.qc.itaojin.annotation.HBaseEntity;

import java.util.Date;

@HBaseEntity(table = "ns1:test_id")
public class Person {

    private int id;
    private String name;
    @HBaseColumn(value = "create_time")
    private Date time;
    @HBaseColumn(value = "order_no")
    private String order;

}
