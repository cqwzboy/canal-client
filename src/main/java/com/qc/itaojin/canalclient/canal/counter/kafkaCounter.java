package com.qc.itaojin.canalclient.canal.counter;

import com.qc.itaojin.canalclient.common.Counter;
import lombok.Data;
import lombok.ToString;

/**
 * @desc kafka消费端试错计数器
 * @author fuqinqin
 * @date 2018-07-06
 */
@Data
@ToString
public class kafkaCounter extends Counter {

    /**
     * 上一次的Canal Server 的 BatchId
     * */
    private long preBatchId;

    public kafkaCounter(){
        this.preBatchId = -1;
    }

    /**
     * 判断给定条件与当前对象持有信息是否相同
     */
    @Override
    public boolean equalsBy(Object... args){
        if(args==null || args.length<=0){
            return false;
        }

        long preBatchId;
        try{
            preBatchId = (long) args[0];
        }catch (Exception e){
            return false;
        }

        if(preBatchId==this.preBatchId){
            return true;
        }

        return false;
    }

    /**
     * 赋值
     * */
    public void set(long batchId){
        this.preBatchId = batchId;
    }

}
