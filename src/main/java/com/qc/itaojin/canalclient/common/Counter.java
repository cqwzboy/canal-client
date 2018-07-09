package com.qc.itaojin.canalclient.common;

import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import lombok.ToString;

/**
 * @desc 计数器
 * @author fuqinqin
 * @date 2018-07-06
 */
@Data
@ToString
public abstract class Counter {

    /**
     * 已尝试次数
     * */
    private int count;

    /**
     * 判断给定条件与当前对象持有信息是否相同
     */
    public abstract boolean equalsBy(Object... args);

    /**
     * 重置计数器
     * */
    public void reset(){
        this.count  = 1;
    }

    /**
     * 计数器加1
     * */
    public void plus(){
        this.count ++;
    }

    /**
     * 次数是否达到规定次数
     * */
    public boolean isUpperLimit(int upperLimit){
        return this.count > upperLimit;
    }

}
