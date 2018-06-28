package com.qc.itaojin.canalclient.enums;

import org.apache.commons.lang.StringUtils;

/**
 * @desc Canal同步过来的数据分为行级数据和表级数据
 * @author fuqinqin
 * @date 2018-06-28
 */
public enum CanalOperationLevelEnum {
    ROW("行级（某张表内）"),
    TABLE("表级"),
    ;

    private final String commont;

    CanalOperationLevelEnum(String commont){
        this.commont = commont;
    }

    public boolean equalsTo(CanalOperationLevelEnum canalOperationLevelEnum){
        if(canalOperationLevelEnum == null){
            return false;
        }

        if(this == canalOperationLevelEnum){
            return true;
        }

        if(this.name().equals(canalOperationLevelEnum.name()) && this.commont.equals(canalOperationLevelEnum.commont)){
            return true;
        }

        return false;
    }

    public static CanalOperationLevelEnum nameOf(String name){
        if(StringUtils.isBlank(name)){
            return null;
        }

        for (CanalOperationLevelEnum canalOperationLevelEnum : CanalOperationLevelEnum.values()) {
            if(name.equals(canalOperationLevelEnum.name())){
                return canalOperationLevelEnum;
            }
        }

        return null;
    }

}
