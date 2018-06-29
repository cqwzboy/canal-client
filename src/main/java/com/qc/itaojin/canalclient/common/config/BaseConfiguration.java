package com.qc.itaojin.canalclient.common.config;

import com.qc.itaojin.canalclient.enums.DataSourceTypeEnum;
import com.qc.itaojin.canalclient.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by fuqinqin on 2018/6/29.
 */
public class BaseConfiguration {

    protected  <T> T invoke(DataSourceTypeEnum dataSourceType, String key, Object... args){
        T t = null;
        try {
            Class clazz = this.getClass();
            Method method = clazz.getMethod(StringUtils.contact("get", dataSourceType.text(), key));
            t = (T) method.invoke(this, args);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        return t;
    }

}
