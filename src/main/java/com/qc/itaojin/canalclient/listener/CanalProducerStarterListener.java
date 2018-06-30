package com.qc.itaojin.canalclient.listener;

import com.qc.itaojin.canalclient.canal.TjkCanalClient;
import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Created by fuqinqin on 2018/6/27.
 */
@Slf4j
public class CanalProducerStarterListener implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        if(ApplicationContextHolder.get() == null){
            ApplicationContextHolder.set(contextRefreshedEvent.getApplicationContext());
        }


        // start tjkCanalClient
        ApplicationContextHolder.getBean("tjkCanalClient", TjkCanalClient.class).start();
        log.info("tjkCanalClient started successfully...");
    }
}
