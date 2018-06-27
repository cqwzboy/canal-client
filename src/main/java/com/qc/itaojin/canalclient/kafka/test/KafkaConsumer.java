package com.qc.itaojin.canalclient.kafka.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Created by fuqinqin on 2018/6/27.
 */
@Component
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = {"testTopic"})
    public void processMessage(String content) {
        log.info("message:{}", content);
        Message message = JSON.parseObject(content, new TypeReference<Message>(){});
        log.info("消费者，message={}",message);
    }

}
