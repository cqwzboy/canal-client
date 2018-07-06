package com.qc.itaojin.canalclient.canal.entity;

import com.qc.itaojin.canalclient.enums.ErrorTypeEnum;
import lombok.Data;

/**
 * @desc 异常记录
 * @author fuqinqin
 * @date 2018-07-06
 */
@Data
public class ErrorEntity {

    /**
     * 异常类型
     * */
    private ErrorTypeEnum errorType;

    /**
     * 业务数据,Json格式
     * */
    private String bizJson;

    /**
     * 堆栈信息
     * */
    private String stackError;

    public ErrorEntity(ErrorTypeEnum errorType){
        this.errorType = errorType;
    }

}
