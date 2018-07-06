package com.qc.itaojin.canalclient.canal.entity;

import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import lombok.ToString;

/**
 * @desc canal客户端试错计数器
 * @author fuqinqin
 * @date 2018-07-06
 */
@Data
@ToString
public class ProgramCounter {

    /**
     * 上一批次首位有效数据的binlog名称
     * */
    private String preBinLogFileName;

    /**
     * 上一批次首位有效数据的binlog位置
     * */
    private long preBinLogOffset;

    /**
     * 已尝试次数
     * */
    private int count;

    public ProgramCounter(){
        this.preBinLogFileName = "";
        this.preBinLogOffset = -1;
    }

    /**
     * 判断给定条件与当前对象持有信息是否相同
     */
    public boolean equalsBy(String binLogFileName, long binLogOffset){
        if(StringUtils.isBlank(binLogFileName) || binLogOffset<0){
            return false;
        }

        if(binLogFileName.equals(this.preBinLogFileName) && binLogOffset==this.preBinLogOffset){
            return true;
        }

        return false;
    }

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

    /**
     * 赋值
     * */
    public void set(String binLogFileName, long binLogOffset){
        this.preBinLogFileName = binLogFileName;
        this.preBinLogOffset = binLogOffset;
    }

}
