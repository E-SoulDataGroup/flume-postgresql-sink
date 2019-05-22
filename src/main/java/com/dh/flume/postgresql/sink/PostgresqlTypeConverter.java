package com.dh.flume.postgresql.sink;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * PostgresqlTypeConverter
 *
 * @author GeZhiHui
 * @create 2019-05-20
 **/

public class PostgresqlTypeConverter implements Serializable {

    private static final List<String> stringTypes = Arrays.asList("uuid", "xml", "cidr", "inet", "macaddr", "varchar");

    private static final List<String> byteTypes = Arrays.asList("bytea", "bit varying");

    private static final List<String> bitTypes = Arrays.asList("bit");

    private static final List<String> doubleTypes = Arrays.asList("money");

    private static final List<String> intTypes = Arrays.asList("int", "int2", "int4", "int8");

    private static final List<String> dateTypes = Arrays.asList("date");

    private static final List<String> timestampTypes = Arrays.asList("timestamp");

    private static final DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter df2 = DateTimeFormat.forPattern("yyyy-MM-dd");

    public static Object convert(Object data, String typeName) {
        if (data == null) {
            return null;
        }
        if (doubleTypes.contains(typeName)) {
            data = Double.parseDouble(String.valueOf(data));
        } else if (bitTypes.contains(typeName)) {
            //
        } else if (stringTypes.contains(typeName)) {
            data = String.valueOf(data);
        } else if (byteTypes.contains(typeName)) {
            data = Byte.valueOf(String.valueOf(data));
        } else if (intTypes.contains(typeName)) {
            if (data instanceof String) {
                data = Integer.parseInt(data.toString());
            }
        } else if(dateTypes.contains(typeName)){
            data = new java.sql.Date(DateTime.parse(String.valueOf(data), df2).toDate().getTime());
        }else if(timestampTypes.contains(typeName)){
            data = new java.sql.Timestamp(DateTime.parse(String.valueOf(data), df).toDate().getTime());
        }
        return data;
    }

}
