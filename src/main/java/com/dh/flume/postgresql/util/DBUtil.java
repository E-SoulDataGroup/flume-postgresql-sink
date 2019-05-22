package com.dh.flume.postgresql.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * DBUtil
 *
 * @author GeZhiHui
 * @create 2018-12-21
 **/

public class DBUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBUtil.class);

    private static final int MAX_RETRY_TIMES = 3;

    private static final Object OBJECT = new Object();

    private DBUtil() {
    }

    private static Connection getConnectionInternal(String url, String username, String password) throws SQLException {
        Connection dbConn;
        synchronized (OBJECT) {
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

    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOGGER.error(ExceptionUtils.getStackTrace(e));
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LOGGER.error(ExceptionUtils.getStackTrace(e));
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.error(ExceptionUtils.getStackTrace(e));
            }
        }
    }

    public static void closeDBResources(ResultSet rs, Collection<PreparedStatement> stmts, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOGGER.error(ExceptionUtils.getStackTrace(e));
            }
        }

        if (!stmts.isEmpty()) {
            try {
                for (PreparedStatement stmt : stmts) {
                    if (stmt != null) {
                        stmt.close();
                    }
                }
            } catch (SQLException e) {
                LOGGER.error(ExceptionUtils.getStackTrace(e));
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.error(ExceptionUtils.getStackTrace(e));
            }
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

    public static List<String> getColumns(String table, Connection dbConn) throws SQLException {
        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, null, table, null);
        while (rs.next()) {
            ret.add(rs.getString("COLUMN_NAME"));
        }
        return ret;
    }

    public static List<String> getColumnTypes(String table, Connection dbConn) throws SQLException {
        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, null, table, null);
        while (rs.next()) {
            ret.add(rs.getString("TYPE_NAME"));
        }
        return ret;
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

}