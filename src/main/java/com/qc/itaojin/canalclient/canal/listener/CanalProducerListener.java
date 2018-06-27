package com.qc.itaojin.canalclient.canal.listener;

import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Created by fuqinqin on 2018/6/27.
 */
@Slf4j
public class CanalProducerListener implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        if(ApplicationContextHolder.get() == null){
            ApplicationContextHolder.set(contextRefreshedEvent.getApplicationContext());
        }

        log.info("================================= haha =====================================");
    }
}