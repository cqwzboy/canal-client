package com.qc.itaojin.canalclient.listener;

import com.qc.itaojin.canalclient.canal.CanalClient;
import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * @desc canal客户端启动类
 * @author fuqinqin
 * @date 2018-07-03
 */
@Slf4j
public class CanalProducerStarterListener implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        if(ApplicationContextHolder.get() == null){
            ApplicationContextHolder.set(contextRefreshedEvent.getApplicationContext());
        }

        // start tjkCanalClient
        ApplicationContextHolder.getBean("canalClient", CanalClient.class).init(DataSourceTypeEnum.TJK).start();
        log.info("tjkCanalClient started successfully...");

        // start aiCanalClient
        ApplicationContextHolder.getBean("canalClient", CanalClient.class).init(DataSourceTypeEnum.AI).start();
        log.info("aiCanalClient started successfully...");
    }
}
