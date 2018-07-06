package com.qc.itaojin.canalclient.mysql.dao.impls;

import com.qc.itaojin.canalclient.canal.entity.ErrorEntity;
import com.qc.itaojin.canalclient.common.dao.BaseDao;
import com.qc.itaojin.canalclient.mysql.dao.IErrorLogDao;
import org.springframework.stereotype.Component;

/**
 * Created by fuqinqin on 2018/7/6.
 */
@Component
public class ErrorLogDaoImpl extends BaseDao implements IErrorLogDao {

    @Override
    public int insert(ErrorEntity errorEntity) {
        String sql = "insert into canal_client_error_log (type, biz_json, stack_error) values (?, ?, ?)";
        Object[] params = {errorEntity.getErrorType().code(), errorEntity.getBizJson(), errorEntity.getStackError()};
        return super.executeUpdate(sql, params);
    }
}
