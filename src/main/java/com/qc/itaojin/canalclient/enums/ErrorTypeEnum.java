package com.qc.itaojin.canalclient.enums;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by fuqinqin on 2018/7/6.
 */
public enum ErrorTypeEnum {

    KAFKA_CONSUME(1, "kafka消费过程中产生的异常"),
    CANAL_LISTEN(2, "canal客户端监听过程中产生的异常"),
    ;

    private int code;
    private String comment;

    ErrorTypeEnum(int code, String comment){
        this.code = code;
        this.comment = comment;
    }

    public int code(){
        return code;
    }

    public static ErrorTypeEnum nameOf(String name){
        if(StringUtils.isBlank(name)){
            return null;
        }

        name = name.toUpperCase();

        for (ErrorTypeEnum errorTypeEnum : ErrorTypeEnum.values()) {
            if(errorTypeEnum.name().equals(name)){
                return errorTypeEnum;
            }
        }

        return null;
    }

    public boolean equalsTo(ErrorTypeEnum type){
        if(type == null){
            return false;
        }

        if(type == this){
            return true;
        }

        if(type.comment.equals(this.comment) && type.name().equals(this.name())){
            return true;
        }

        return false;
    }

}
