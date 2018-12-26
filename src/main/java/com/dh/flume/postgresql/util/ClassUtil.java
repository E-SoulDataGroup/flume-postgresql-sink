package com.dh.flume.postgresql.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;

/**
 * ClassUtil
 *
 * @author GeZhiHui
 * @create 2018-12-21
 **/

public class ClassUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClassUtil.class);

    public  static final String LOCK_STR = "jdbc_lock_str";

    private ClassUtil(){}

    public static void forName(String clazz, ClassLoader classLoader)  {
        synchronized (LOCK_STR){
            try {
                Class.forName(clazz, true, classLoader);
                DriverManager.setLoginTimeout(10);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    public synchronized static void forName(String clazz) {
        try {
            Class<?> driverClass = Class.forName(clazz);
            driverClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}