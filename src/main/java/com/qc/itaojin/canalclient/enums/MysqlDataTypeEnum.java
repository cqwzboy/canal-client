package com.qc.itaojin.canalclient.enums;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by fuqinqin on 2018/6/25.
 */
public enum MysqlDataTypeEnum {
    BIT("bit"),
    TINYINT("tinyint"),
    SMALLINT("smallint"),
    MEDIUMINT("mediumint"),
    INT("int"),
    BIGINT("bigint"),

    FLOAT("float"),
    DOUBLE("double"),
    DECIMAL("decimal"),

    YEAR("year"),
    TIME("time"),
    DATE("date"),
    DATETIME("datetime"),
    TIMESTAMP("timestamp"),

    TINYTEXT("tinytext"),
    TEXT("text"),
    MEDIUMTEXT("mediumtext"),
    LONGTEXT("longtext"),

    CHAR("char"),
    VARCHAR("varchar"),

    BINARY("binary"),
    VARBINARY("varbinary"),
    BLOG("blog"),
    TINYBLOG("tinyblog"),
    MEDIUMBLOG("mediumblog"),
    LONGBLOG("longblog")
    ;

    private String text;

    MysqlDataTypeEnum(String text) {
        this.text = text;
    }

    public static MysqlDataTypeEnum nameOf(String name){
        if(StringUtils.isBlank(name)){
            return null;
        }

        name = name.toUpperCase();

        for (MysqlDataTypeEnum MysqlDataTypeEnum : MysqlDataTypeEnum.values()) {
            if(MysqlDataTypeEnum.name().equals(name)){
                return MysqlDataTypeEnum;
            }
        }

        return null;
    }

    public static boolean exists(MysqlDataTypeEnum type){
        if(type == null){
            return false;
        }

        for (MysqlDataTypeEnum MysqlDataTypeEnum : MysqlDataTypeEnum.values()) {
            if(MysqlDataTypeEnum.equalsTo(type)){
                return true;
            }
        }

        return false;
    }

    public boolean equalsTo(MysqlDataTypeEnum type){
        if(type == null){
            return false;
        }

        if(type == this){
            return true;
        }

        if(type.text.equals(this.text) && type.name().equals(this.name())){
            return true;
        }

        return false;
    }

    public static String transToHiveDataType(MysqlDataTypeEnum type){
        if(type == null){
            return "string";
        }

        if(type.equalsTo(MysqlDataTypeEnum.TINYINT)){
            return "tinyint";
        }

        if(type.equalsTo(MysqlDataTypeEnum.SMALLINT)){
            return "smallint";
        }

        if(type.equalsTo(MysqlDataTypeEnum.INT) || type.equalsTo(MysqlDataTypeEnum.YEAR) || type.equalsTo(MysqlDataTypeEnum.MEDIUMINT)
                || type.equalsTo(MysqlDataTypeEnum.BIT)){
            return "int";
        }

        if(type.equalsTo(MysqlDataTypeEnum.BIGINT)){
            return "bigint";
        }

        if(type.equalsTo(MysqlDataTypeEnum.FLOAT)){
            return "float";
        }

        if(type.equalsTo(MysqlDataTypeEnum.DOUBLE) || type.equalsTo(MysqlDataTypeEnum.DECIMAL)){
            return "double";
        }

        // hive v0.8.0 + 才支持的数据类型
        if(type.equalsTo(MysqlDataTypeEnum.TIME) || type.equalsTo(MysqlDataTypeEnum.DATE)
                || type.equalsTo(MysqlDataTypeEnum.DATETIME)
                || type.equalsTo(MysqlDataTypeEnum.TIMESTAMP)){
            return "timestamp";
        }

        /*if(type.equalsTo(MysqlDataTypeEnum.TINYTEXT) || type.equalsTo(MysqlDataTypeEnum.TEXT)
                || type.equalsTo(MysqlDataTypeEnum.MEDIUMTEXT)
                || type.equalsTo(MysqlDataTypeEnum.LONGTEXT)
                || type.equalsTo(MysqlDataTypeEnum.CHAR)
                || type.equalsTo(MysqlDataTypeEnum.VARCHAR)){
            return "string";
        }*/

        // hive v0.8.0 + 才支持的数据类型
        if(type.equalsTo(MysqlDataTypeEnum.BINARY) || type.equalsTo(MysqlDataTypeEnum.VARBINARY)
                || type.equalsTo(MysqlDataTypeEnum.BLOG)
                || type.equalsTo(MysqlDataTypeEnum.TINYBLOG)
                || type.equalsTo(MysqlDataTypeEnum.MEDIUMBLOG)
                || type.equalsTo(MysqlDataTypeEnum.LONGBLOG)){
            return "binary";
        }

        return "string";
    }
}
