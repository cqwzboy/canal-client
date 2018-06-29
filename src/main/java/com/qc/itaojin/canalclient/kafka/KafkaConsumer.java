package com.qc.itaojin.canalclient.kafka;

import com.qc.itaojin.canalclient.canal.entity.CanalOperationEntity;
import com.qc.itaojin.canalclient.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Created by fuqinqin on 2018/6/27.
 */
@Component
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = {"hello"})
    public void processMessage(String content) {
        log.info("consumer message:{}", content);

        CanalOperationEntity operationEntity = JsonUtil.parse(content, CanalOperationEntity.class);
        log.info("operationEntity: {}", operationEntity.toString());
    }

}
