package com.qc.itaojin.canalclient.canal.counter;

import com.qc.itaojin.canalclient.common.Counter;
import lombok.Data;
import lombok.ToString;

/**
 * @desc canal客户端试错计数器
 * @author fuqinqin
 * @date 2018-07-06
 */
@Data
@ToString
public class CanalCounter extends Counter {

    /**
     * 上一批次首位有效数据的binlog名称
     * */
    private String preBinLogFileName;

    /**
     * 上一批次首位有效数据的binlog位置
     * */
    private long preBinLogOffset;

    public CanalCounter(){
        this.preBinLogFileName = "";
        this.preBinLogOffset = -1;
    }

    /**
     * 判断给定条件与当前对象持有信息是否相同
     */
    @Override
    public boolean equalsBy(Object... args){
        if(args==null || args.length<=0){
            return false;
        }

        String binLogFileName;
        long binLogOffset;
        try{
            binLogFileName = (String) args[0];
            binLogOffset = (long) args[1];
        }catch (Exception e){
            return false;
        }

        if(binLogFileName.equals(this.preBinLogFileName) && binLogOffset==this.preBinLogOffset){
            return true;
        }

        return false;
    }

    /**
     * 赋值
     * */
    public void set(String binLogFileName, long binLogOffset){
        this.preBinLogFileName = binLogFileName;
        this.preBinLogOffset = binLogOffset;
    }

}
