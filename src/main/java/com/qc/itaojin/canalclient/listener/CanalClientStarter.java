package com.qc.itaojin.canalclient.listener;

import com.qc.itaojin.canalclient.canal.CanalClient;
import com.qc.itaojin.canalclient.canal.CanalClient.CanalClientFactory;
import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.common.Constants;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.common.ItaojinZKConstants;
import com.qc.itaojin.common.WatcherRegister;
import com.qc.itaojin.entity.ZKNodeInfoWrapper;
import com.qc.itaojin.service.IZookeeperService;
import com.qc.itaojin.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;


/**
 * @desc canal客户端启动类
 * @author fuqinqin
 * @date 2018-07-03
 */
@Slf4j
public class CanalClientStarter implements ApplicationListener<ContextRefreshedEvent> {

    private IZookeeperService zookeeperService;
    private WatcherRegister watcherRegister;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        if(ApplicationContextHolder.get() == null){
            ApplicationContextHolder.set(contextRefreshedEvent.getApplicationContext());
        }

        zookeeperService = ApplicationContextHolder.getBean("zookeeperService", IZookeeperService.class);
        watcherRegister = ApplicationContextHolder.getBean("watcherRegister", WatcherRegister.class);

        // canal 客户端集合
        List<CanalClient> tasks = CanalClientFactory.get(
                Arrays.asList(
                        DataSourceTypeEnum.TJK,
                        DataSourceTypeEnum.PAY,
                        DataSourceTypeEnum.AI,
                        DataSourceTypeEnum.BENCH
                )
        );

        try {
            ZKNodeInfoWrapper wrapper = ZKNodeInfoWrapper.build();
            if(zookeeperService.registerLeader(Constants.EPHEMERAL_PATH, JsonUtil.toJson(wrapper).getBytes(ItaojinZKConstants.CHARSET))){
                for (CanalClient task : tasks) {
                    task.start();
                }
                log.info("canalClient started successfully...");
            }else{
                watcherRegister.enableListen(Constants.EPHEMERAL_PATH, tasks);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
