package com.qc.itaojin.canalclient.listener;

import com.qc.itaojin.canalclient.canal.CanalClient;
import com.qc.itaojin.canalclient.common.ApplicationContextHolder;
import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.common.ItaojinZKConstants;
import com.qc.itaojin.common.WatcherRegister;
import com.qc.itaojin.entity.ZKNodeInfoWrapper;
import com.qc.itaojin.service.IZookeeperService;
import com.qc.itaojin.util.InetAddressUtil;
import com.qc.itaojin.util.JsonUtil;
import com.qc.itaojin.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

    private static final String path = "/itaojin/qc/canal-client/running";

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        if(ApplicationContextHolder.get() == null){
            ApplicationContextHolder.set(contextRefreshedEvent.getApplicationContext());
        }

        zookeeperService = ApplicationContextHolder.getBean("zookeeperService", IZookeeperService.class);
        watcherRegister = ApplicationContextHolder.getBean("watcherRegister", WatcherRegister.class);

        /**
         * 初始化
         * */
        // 竞争节点（临时节点）
        watcherRegister.setPath(path);
        List<CanalClient> tasks = new ArrayList<>();
        tasks.add(ApplicationContextHolder.getBean("canalClient", CanalClient.class).init(DataSourceTypeEnum.TJK));
//        tasks.add(ApplicationContextHolder.getBean("canalClient", CanalClient.class).init(DataSourceTypeEnum.PAY));
//        tasks.add(ApplicationContextHolder.getBean("canalClient", CanalClient.class).init(DataSourceTypeEnum.AI));
//        tasks.add(ApplicationContextHolder.getBean("canalClient", CanalClient.class).init(DataSourceTypeEnum.BENCH));
        // 需被唤醒的任务
        watcherRegister.setTasks(tasks);

        // 初始化zookeeper中的节点
        initDirectory(path);

        try {
            ZKNodeInfoWrapper wrapper = ZKNodeInfoWrapper.build();
            if(zookeeperService.registerLeader(path, JsonUtil.toJson(wrapper).getBytes(ItaojinZKConstants.CHARSET))){
                for (CanalClient task : tasks) {
                    task.start();
                }
                log.info("canalClient started successfully...");
            }else{
                watcherRegister.start();
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void initDirectory(String path) {
        if(StringUtils.isBlank(path)){
            throw new IllegalArgumentException("zookeeper inition directory is null");
        }
        if(path.indexOf("/") == -1){
            throw new IllegalArgumentException("zookeeper inition directory must contains '/'");
        }

        String[] paths = path.split("/");
        String p = "";
        // 忽略最后一层临时节点
        for (int i=0;i<paths.length-1;i++) {
            String node = paths[i];
            if(StringUtils.isBlank(node)){
                continue;
            }
            p = StringUtils.contact(p, "/", node);
            zookeeperService.create(p, null);
        }
    }
}
