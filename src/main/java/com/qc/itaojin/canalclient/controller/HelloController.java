package com.qc.itaojin.canalclient.controller;

import com.qc.itaojin.canalclient.kafka.test.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.ws.RequestWrapper;

/**
 * Created by fuqinqin on 2018/6/27.
 */
@RestController
@RequestMapping("/hello")
public class HelloController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @GetMapping("")
    public String sayHello(){
        return "Hello World";
    }

    @GetMapping("/test")
    public String test(){
        kafkaProducer.send(1,"zhangsan",23);
        return "success";
    }

}
