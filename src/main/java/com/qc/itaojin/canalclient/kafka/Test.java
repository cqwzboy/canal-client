package com.qc.itaojin.canalclient.kafka;

import com.qc.itaojin.canalclient.kafka.test.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by fuqinqin on 2018/6/27.
 */
@Slf4j
public class Test {

    public static void main(String[] args){
        Message person = new Message();
        person.setName("wangda");
        log.info("person'name is {}", person.getName());
    }

}
