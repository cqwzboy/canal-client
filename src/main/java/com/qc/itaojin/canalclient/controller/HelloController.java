package com.qc.itaojin.canalclient.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
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
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("")
    public String sayHello(){
        return "Hello World";
    }

    @GetMapping("/test")
    public String test(){
        kafkaTemplate.send("hello", "HelloController's test");
        return "success";
    }

}
