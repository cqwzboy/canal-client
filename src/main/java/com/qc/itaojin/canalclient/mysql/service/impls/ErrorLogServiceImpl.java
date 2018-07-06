package com.qc.itaojin.canalclient.mysql.service.impls;

import com.qc.itaojin.canalclient.canal.entity.ErrorEntity;
import com.qc.itaojin.canalclient.common.BaseService;
import com.qc.itaojin.canalclient.mysql.dao.IErrorLogDao;
import com.qc.itaojin.canalclient.mysql.service.IErrorLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by fuqinqin on 2018/7/6.
 */
@Service
public class ErrorLogServiceImpl extends BaseService implements IErrorLogService {

    @Autowired
    private IErrorLogDao errorLogDao;

    @Override
    public boolean insert(ErrorEntity errorEntity) {
        if(errorEntity == null){
            return true;
        }
        return errorLogDao.insert(errorEntity)==1;
    }
}
