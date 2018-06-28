package com.qc.itaojin.canalclient;

import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.listener.CanalProducerStarterListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class CanalClientApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(CanalClientApplication.class);
        springApplication.addListeners(new CanalProducerStarterListener());
        ApplicationContext applicationContext = springApplication.run(args);
        ApplicationContextHolder.set(applicationContext);
    }
}
