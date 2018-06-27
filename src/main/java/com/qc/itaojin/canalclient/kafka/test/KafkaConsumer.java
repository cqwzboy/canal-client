package com.qc.itaojin.canalclient.kafka.test;

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
//        Message message = JSON.parseObject(content, new TypeReference<Message>(){});
//        log.info("消费者，message={}",message);
    }

}
