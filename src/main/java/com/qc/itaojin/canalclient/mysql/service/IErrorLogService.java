package com.qc.itaojin.canalclient.mysql.service;

import com.qc.itaojin.canalclient.canal.entity.ErrorEntity;

/**
 * Created by fuqinqin on 2018/7/6.
 */
public interface IErrorLogService {

    /**
     * 添加记录
     * */
    boolean insert(ErrorEntity errorEntity);

}
