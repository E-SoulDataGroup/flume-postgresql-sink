package com.dh.flume.postgresql.util;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * SysUtil
 *
 * @author GeZhiHui
 * @create 2018-12-21
 **/

public class SysUtil {

    private SysUtil() { }

    public static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
