package com.qc.itaojin.canalclient.common.config;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * Created by fuqinqin on 2018/7/2.
 */
@Component
@ConfigurationProperties(prefix = "hbase")
@PropertySource(value = "classpath:local.yaml")
@Data
public class HBaseConfiguration {

    @Value("${master}")
    private String master;

    @Autowired
    private ZookeeperConfiguration zookeeperConfiguration;

    /**
     * HBase配置类
     * */
    private Configuration configuration;

    @PostConstruct
    public void init(){
        configuration = org.apache.hadoop.hbase.HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(zookeeperConfiguration.getPort()));
        configuration.set("hbase.zookeeper.quorum", zookeeperConfiguration.getQuorum());
        configuration.set("hbase.master", master);
    }

    public HBaseAdmin getAdmin(){
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return admin;
    }

    public void closeAdmin(HBaseAdmin admin){
        if(admin != null){
            try {
                admin.close();
            } catch (IOException e) {

            }
        }
    }

}
