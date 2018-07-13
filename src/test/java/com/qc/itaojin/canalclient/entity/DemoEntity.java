package com.qc.itaojin.canalclient.entity;

import com.qc.itaojin.annotation.HBaseColumn;
import com.qc.itaojin.annotation.HBaseEntity;
import lombok.Data;
import lombok.ToString;

import java.util.Date;

/**
 * Created by fuqinqin on 2018/7/13.
 */
@Data
@ToString
@HBaseEntity(table = "ns1:test_id")
public class DemoEntity {

    private int id;
    private String name;
    @HBaseColumn(value = "create_time")
    private Date createTime;
    private String orderNo;
    private int age;

}
