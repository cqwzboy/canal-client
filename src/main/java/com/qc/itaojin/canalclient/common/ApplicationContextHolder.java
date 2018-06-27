package com.qc.itaojin.canalclient.common;

import org.springframework.context.ApplicationContext;

/**
 * Created by fuqinqin on 2018/6/27.
 */
public class ApplicationContextHolder {

    private static ApplicationContext applicationContext;

    public static void set(ApplicationContext context){
        applicationContext = context;
    }

    public static ApplicationContext get(){
        return applicationContext;
    }

    public static <T> T getBean(String name, Class<T> clazz){
        if(applicationContext == null){
            return null;
        }

        return applicationContext.getBean(name, clazz);
    }

}
