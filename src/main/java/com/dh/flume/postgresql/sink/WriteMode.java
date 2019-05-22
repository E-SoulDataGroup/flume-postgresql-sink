package com.dh.flume.postgresql.sink;

/**
 * WriteMode
 *
 * @author GeZhiHui
 * @create 2019-05-20
 **/

public enum WriteMode {

    INSERT("insert"),

    UPDATE("update"),

    REPLACE("replace");

    private String mode;

    WriteMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

}
