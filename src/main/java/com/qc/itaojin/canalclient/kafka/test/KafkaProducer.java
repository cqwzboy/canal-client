package com.qc.itaojin.canalclient.kafka.test;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Created by fuqinqin on 2018/6/27.
 */
@Component
@Slf4j
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public boolean send(Integer id, String name, Integer age){
        Message message = new Message();
        message.setId(id);
        message.setName(name);
        message.setAge(age);
        return send(message);
    }

    public boolean send(Message message){
        log.info("message = {}",message.toString());
        kafkaTemplate.send("testTopic", JSON.toJSONString(message));
        return true;
    }

}
