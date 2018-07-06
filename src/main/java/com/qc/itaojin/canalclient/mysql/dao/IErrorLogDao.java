package com.qc.itaojin.canalclient.mysql.dao;

import com.qc.itaojin.canalclient.canal.entity.ErrorEntity;

/**
 * Created by fuqinqin on 2018/7/6.
 */
public interface IErrorLogDao {

    int insert(ErrorEntity errorEntity);

}
