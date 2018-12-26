package com.dh.flume.postgresql.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JsonUtil
 *
 * @author GeZhiHui
 * @create 2018-12-21
 **/

public class JsonUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonUtil.class);

    private JsonUtil() {
    }


    public static boolean isJosn(String str) {
        boolean flag = false;
        if (StringUtils.isNotBlank(str)) {
            try {
                JSONObject.parseObject(str);
                flag = true;
            } catch (Exception e) {
                LOGGER.error("解析json失败 --> {}", ExceptionUtils.getFullStackTrace(e));
                LOGGER.error("json格式错误 {}", str);
            }
        }
        return flag;
    }

    public static boolean isJosnArray(String str) {
        boolean flag = false;
        if (StringUtils.isNotBlank(str)) {
            try {
                JSONObject.parseArray(str);
                flag = true;
            } catch (Exception e) {
                LOGGER.error("解析json失败 --> {}", ExceptionUtils.getFullStackTrace(e));
                LOGGER.error("json格式错误 {}", str);
            }
        }
        return flag;
    }
}
