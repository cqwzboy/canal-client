package com.qc.itaojin.canalclient.kafka;

import com.qc.itaojin.canalclient.canal.entity.ErrorEntity;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.mysql.service.IErrorLogService;
import com.qc.itaojin.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @desc kafka消费类
 * @author fuqinqin
 * @date 2018-07-03
 */
@Component
@Slf4j
public class ErrorConsumer {

    @Autowired
    private IErrorLogService errorLogService;

    /**
     * 物理数据库业务类型
     * */
    private DataSourceTypeEnum ID;

    @KafkaListener(topics = "errorLog")
    public void logError(String error){
        log.info("error message:{}", error);

        ErrorEntity errorEntity = JsonUtil.parse(error, ErrorEntity.class);
        errorLogService.insert(errorEntity);
        log.info("记录错误日志成功！");
    }

}
