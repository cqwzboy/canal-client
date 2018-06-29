package com.qc.itaojin.canalclient.enums;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by fuqinqin on 2018/6/26.
 */
public enum KeyTypeEnum {
    NONE("无主键"),
    PRIMARY_KEY("单主键"),
    COMBINE_KEY("联合主键"),
    ;

    private String text;

    KeyTypeEnum(String text){
        this.text = text;
    }

    public static KeyTypeEnum nameOf(String name){
        if(StringUtils.isBlank(name)){
            return null;
        }

        name = name.toUpperCase();

        for (KeyTypeEnum keyType : KeyTypeEnum.values()) {
            if(keyType.name().equals(name)){
                return keyType;
            }
        }

        return null;
    }

    public boolean equalsTo(KeyTypeEnum type){
        if(type == null){
            return false;
        }

        if(type == this){
            return true;
        }

        if(type.text.equals(this.text) && type.name().equals(this.name())){
            return true;
        }

        return false;
    }

}
