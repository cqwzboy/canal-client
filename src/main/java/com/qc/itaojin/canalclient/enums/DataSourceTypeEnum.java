package com.qc.itaojin.canalclient.enums;

import org.apache.commons.lang.StringUtils;

/**
 * @desc 物理数据库服务器类型
 * @author fuqinqin
 * @date 2018-06-29
 */
public enum DataSourceTypeEnum {
    //淘金客物理服务器(主要)
    TJK("Tjk"),
    //语音物理服务器
    AI("Ai"),
    //支付系统物理服务器
    PAY("Pay"),
    //工作台物理服务器
    BENCH("Bench"),
    ;

    private String text;

    DataSourceTypeEnum(String text){
        this.text = text;
    }

    public String text(){
        return text;
    }

    public boolean equalsTo(DataSourceTypeEnum dataSourceEnum){
        if(dataSourceEnum == null){
            return false;
        }

        if(this == dataSourceEnum){
            return true;
        }

        if(this.name().equals(dataSourceEnum.name()) && this.text.equals(dataSourceEnum.text)){
            return true;
        }

        return false;
    }

    public static DataSourceTypeEnum nameOf(String name){
        if(StringUtils.isBlank(name)){
            return null;
        }

        for (DataSourceTypeEnum dataSourceEnum : DataSourceTypeEnum.values()) {
            if(name.equals(dataSourceEnum.name())){
                return dataSourceEnum;
            }
        }

        return null;
    }
}
