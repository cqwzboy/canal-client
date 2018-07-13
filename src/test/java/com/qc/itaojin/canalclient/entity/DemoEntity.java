package com.qc.itaojin.canalclient.entity;

import lombok.Data;
import lombok.ToString;

import java.util.Date;

/**
 * Created by fuqinqin on 2018/7/13.
 */
@Data
@ToString
public class DemoEntity {

    private int id;
    private String name;
    private Date createTime;
    private String orderNo;
    private int age;

}
