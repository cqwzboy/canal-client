package com.qc.itaojin.canalclient.common;

/**
 * Created by fuqinqin on 2018/6/26.
 */
public class Constants {

    /**
     * 日志样板
     * 第一个占位符放置线程id
     * 第二个占位符放置业务ID
     * 第三个占位符放置正常输出
     * */
    public static final String LOG_TEMPLATE = "[ %d ] [ %s ] %s";

    /**
     * canal客户端试错次数
     * */
    public static final int RETRY_NUMBER = 5;

    /**
     * Hive相关的常量
     * */
    public interface HiveConstants{
        /**
         * 默认数据库
         * */
        String DEFAULT_SCHEMA = "default";
    }

    /**
     * kafka相关常量
     * */
    public interface KafkaConstants{
        /**
         * normal_topic
         * */
        String NORMAL_TOPIC = "itaojin_bigdata";

        /**
         * error_topic
         * */
        String ERROR_TOPIC = "errorLog";
    }

}