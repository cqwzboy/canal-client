package com.qc.itaojin.canalclient.enums;

import org.apache.commons.lang.StringUtils;

/**
 * @desc Canal同步过来的数据可能是行级增删改，也可能是表级增删改
 * @author fuqinqin
 * @date 2018-06-28
 */
public enum CanalOperationTypeEnum {
    CREATE("新增 数据/表"),
    DELETE("删除 数据"),
    UPDATE("修改 数据/表")
    ;

    private final String commont;

    CanalOperationTypeEnum(String commont){
        this.commont = commont;
    }

    public boolean equalsTo(CanalOperationTypeEnum canalOperationTypeEnum){
        if(canalOperationTypeEnum == null){
            return false;
        }

        if(this == canalOperationTypeEnum){
            return true;
        }

        if(this.name().equals(canalOperationTypeEnum.name()) && this.commont.equals(canalOperationTypeEnum.commont)){
            return true;
        }

        return false;
    }

    public static CanalOperationTypeEnum nameOf(String name){
        if(StringUtils.isBlank(name)){
            return null;
        }

        for (CanalOperationTypeEnum canalOperationTypeEnum : CanalOperationTypeEnum.values()) {
            if(name.equals(canalOperationTypeEnum.name())){
                return canalOperationTypeEnum;
            }
        }

        return null;
    }

}
