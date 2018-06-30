package com.qc.itaojin.canalclient;

import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.listener.CanalProducerStarterListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan(value = {"com.qc.itaojin.canalclient.*"})
public class CanalClientApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(CanalClientApplication.class);
        springApplication.addListeners(new CanalProducerStarterListener());
        ApplicationContext applicationContext = springApplication.run(args);
        ApplicationContextHolder.set(applicationContext);
    }
}
