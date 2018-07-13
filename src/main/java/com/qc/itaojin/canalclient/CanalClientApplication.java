package com.qc.itaojin.canalclient;

import com.qc.itaojin.annotation.EnableHBaseTableCheck;
import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.listener.CanalClientStarter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication()
@EnableHBaseTableCheck(scan = {"com.qc.itaojin.canalcllient.test"})
public class CanalClientApplication {

    public static void main(String[] args) {
        SpringApplication springApplication = new SpringApplication(CanalClientApplication.class);
        springApplication.addListeners(new CanalClientStarter());
        ApplicationContext applicationContext = springApplication.run(args);
        ApplicationContextHolder.set(applicationContext);
    }

}
