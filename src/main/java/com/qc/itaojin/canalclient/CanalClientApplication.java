package com.qc.itaojin.canalclient;

import com.qc.itaojin.canalclient.canal.listener.CanalProducerListener;
import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.kafka.test.KafkaConsumer;
import com.qc.itaojin.canalclient.kafka.test.KafkaProducer;
import com.qc.itaojin.canalclient.kafka.test.Message;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class CanalClientApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(CanalClientApplication.class);
        springApplication.addListeners(new CanalProducerListener());
        ApplicationContext applicationContext = springApplication.run(args);
        ApplicationContextHolder.set(applicationContext);

//        SpringApplication.run(CanalClientApplication.class, args);
    }
}
