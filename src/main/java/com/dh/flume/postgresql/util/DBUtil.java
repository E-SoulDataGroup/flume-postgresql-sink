package com.dh.flume.postgresql.util;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DBUtil
 *
 * @author GeZhiHui
 * @create 2018-12-21
 **/

public class DBUtil {

    private static final int MAX_RETRY_TIMES = 3;

    private DBUtil() {
    }

    private static Connection getConnectionInternal(String url, String username, String password) throws SQLException {
        Connection dbConn;
        synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);

            if (username == null) {
                dbConn = DriverManager.getConnection(url);
            } else {
                dbConn = DriverManager.getConnection(url, username, password);
            }
        }

        return dbConn;
    }

    public static Connection getConnection(String url, String username, String password) throws SQLException {
        if (!url.startsWith("jdbc:mysql")) {
            return getConnectionInternal(url, username, password);
        } else {
            boolean failed = true;
            Connection dbConn = null;
            for (int i = 0; i < MAX_RETRY_TIMES && failed; ++i) {
                try {
                    dbConn = getConnectionInternal(url, username, password);
                    dbConn.createStatement().execute("select 111");
                    failed = false;
                } catch (Exception e) {
                    if (dbConn != null) {
                        dbConn.close();
                    }

                    if (i == MAX_RETRY_TIMES - 1) {
                        throw e;
                    } else {
                        SysUtil.sleep(3000);
                    }
                }
            }

            return dbConn;
        }
    }


    public static List<Map<String, Object>> executeQuery(Connection connection, String sql) {
        List<Map<String, Object>> result = com.google.common.collect.Lists.newArrayList();
        ResultSet res = null;
        Statement statement = null;
        try {
            statement = connection.createStatement();
            res = statement.executeQuery(sql);
            int columns = res.getMetaData().getColumnCount();
            List<String> columnName = com.google.common.collect.Lists.newArrayList();
            for (int i = 0; i < columns; i++) {
                columnName.add(res.getMetaData().getColumnName(i + 1));
            }

            while (res.next()) {
                Map<String, Object> row = com.google.common.collect.Maps.newHashMap();
                for (int i = 0; i < columns; i++) {
                    row.put(columnName.get(i), res.getObject(i + 1));
                }
                result.add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            DBUtil.closeDBResources(res, statement, null);
        }
        return result;
    }

    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }

    public static void executeBatch(Connection dbConn, List<String> sqls) {
        if (sqls == null || sqls.size() == 0) {
            return;
        }

        try {
            Statement stmt = dbConn.createStatement();
            for (String sql : sqls) {
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void executeOneByOne(Connection dbConn, List<String> sqls) {
        if (sqls == null || sqls.size() == 0) {
            return;
        }

        try {
            Statement stmt = dbConn.createStatement();
            for (String sql : sqls) {
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, List<String>> getPrimaryOrUniqueKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> keyMap = new HashMap<>();
        DatabaseMetaData meta = dbConn.getMetaData();
        ResultSet rs = meta.getIndexInfo(null, null, table, true, false);
        while (rs.next()) {
            String pkName = rs.getString(6);
            String columnName = rs.getString(9);
            if (!keyMap.containsKey(pkName)) {
                keyMap.put(pkName, new ArrayList<>());
            }
            keyMap.get(pkName).add(columnName);
        }
        return keyMap;
    }


    public static void setParameterValue(Object param, PreparedStatement statement, int i) throws SQLException {
        if (param instanceof String) {
            statement.setString(i + 1, (String) param);
        } else if (param instanceof Long) {
            statement.setLong(i + 1, (Long) param);
        } else if (param instanceof Integer) {
            statement.setInt(i + 1, (Integer) param);
        } else if (param instanceof Double) {
            statement.setDouble(i + 1, (Double) param);
        } else if (param instanceof Boolean) {
            statement.setBoolean(i + 1, (Boolean) param);
        } else if (param instanceof Float) {
            statement.setFloat(i + 1, (Float) param);
        } else if (param instanceof BigDecimal) {
            statement.setBigDecimal(i + 1, (BigDecimal) param);
        } else if (param instanceof Byte) {
            statement.setByte(i + 1, (Byte) param);
        } else if (param instanceof Short) {
            statement.setShort(i + 1, (Short) param);
        } else if (param instanceof Date) {
            statement.setDate(i + 1, (Date) param);
        } else if (param instanceof Time) {
            statement.setTime(i + 1, (Time) param);
        } else if (param instanceof Timestamp) {
            statement.setTimestamp(i + 1, (Timestamp) param);
        } else if (param instanceof Array) {
            statement.setArray(i + 1, (Array) param);
        } else {
            //extends with other types if needed
            throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet).");
        }
    }


    public static String formatJdbcUrl(String pluginName, String dbUrl) {
        if (pluginName.equalsIgnoreCase("mysqlreader")
                || pluginName.equalsIgnoreCase("mysqldreader")
                || pluginName.equalsIgnoreCase("postgresqlreader")) {
            String[] splits = dbUrl.split("\\?");

            Map<String, String> paramMap = new HashMap<String, String>();
            if (splits.length > 1) {
                String[] pairs = splits[1].split("&");
                for (String pair : pairs) {
                    String[] leftRight = pair.split("=");
                    paramMap.put(leftRight[0], leftRight[1]);
                }
            }

            paramMap.put("useCursorFetch", "true");

            if (pluginName.equalsIgnoreCase("mysqlreader")
                    || pluginName.equalsIgnoreCase("mysqldreader")) {
                paramMap.put("zeroDateTimeBehavior", "convertToNull");
            }

            StringBuffer sb = new StringBuffer(splits[0]);
            if (paramMap.size() != 0) {
                sb.append("?");
                int index = 0;
                for (Map.Entry<String, String> entry : paramMap.entrySet()) {
                    if (index != 0) {
                        sb.append("&");
                    }
                    sb.append(entry.getKey() + "=" + entry.getValue());
                    index++;
                }
            }

            dbUrl = sb.toString();
        }

        return dbUrl;
    }
}