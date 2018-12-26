package com.dh.flume.postgresql.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dh.flume.postgresql.util.DBUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * PostgreSQLSink
 *
 * @author GeZhiHui
 * @create 2018-12-21
 **/

public class PostgreSQLSink extends AbstractSink implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSink.class);

    private String hostname;
    private String port;
    private String database;
    private String user;
    private String password;
    private Integer batchSize;

    private String tableNameKey;

    private Statement stmt;
    private Connection conn;

    private static final String URL = "jdbc:postgresql://${hostname}:${port}/${database}";

    @Override
    public synchronized void start() {
        super.start();
        String hostURL = StringUtils.replace(URL, "${hostname}", hostname);
        String portURL = StringUtils.replace(hostURL, "${port}", port);
        String url = StringUtils.replace(portURL, "${database}", database);
        try {
            conn = DBUtil.getConnection(url, user, password);
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
        } catch (SQLException e) {
            LOGGER.error("获取pg连接失败 --> {}", ExceptionUtils.getFullStackTrace(e));
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        DBUtil.closeDBResources(null, stmt, conn);
    }

    @Override
    public void configure(Context context) {
        hostname = context.getString("hostname");
        Preconditions.checkNotNull(hostname, "hostname must be set!!");
        port = context.getString("port");
        Preconditions.checkNotNull(port, "port must be set!!");
        database = context.getString("database");
        Preconditions.checkNotNull(database, "database must be set!!");
        user = context.getString("user");
        Preconditions.checkNotNull(user, "user must be set!!");
        password = context.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        tableNameKey = context.getString("tableNameKey", "topic");
        batchSize = context.getInteger("batchSize", 100);

    }

    @Override
    public Status process() {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event;
        String body;
        Map<String, String> header;
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            clearBatch();
            for (int i = 0; i < batchSize; i++) {
                List<List> actions = Lists.newArrayList();
                List<String> columns = new ArrayList<>();
                event = channel.take();
                if (event != null) {
                    header = event.getHeaders();
                    //LOGGER.info("[header]: {}", header.toString());
                    //TODO 表名的获取,根据header判断输出到哪张mysql表中
                    String tableName = header.get(tableNameKey);
                    if (tableName.contains("dh_ods_ods_dh_")) {
                        String[] ss = tableName.split("dh_ods_ods_dh_");
                        //tableName = "tab_" + ss[1];
                        if(ss[1].contains("advert_click")){
                            tableName = "ad_click";
                        }else if(ss[1].contains("appstart")){
                            tableName = "sdk_app_start";
                        }else{
                            tableName = ss[1];
                        }

                    }
                    body = new String(event.getBody());
                    parseData(body, actions, columns);
                    execute(actions, columns, tableName);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            transaction.commit();
        } catch (Exception e) {
            try {
                if (transaction != null) {
                    transaction.rollback();
                }
            } catch (Exception e2) {
                LOGGER.error("Exception in rollback. Rollback might not have been successful.", e2);
            }
            LOGGER.error("Failed to commit transaction. Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

        return result;
    }

    private void parseData(String body, List<List> actions, List<String> columns) {
        Object object = JSONObject.parse(body);
        List<Object> rows = new ArrayList<>();
        if (object instanceof JSONObject) {
            JSONObject bodyJSON = (JSONObject) object;
            Set<String> keySet = bodyJSON.keySet();
            columns.addAll(keySet);
            fillData(actions, rows, bodyJSON);
        }
        if (object instanceof JSONArray) {
            JSONArray bodyJSON = (JSONArray) object;
            for (int j = 0; j < bodyJSON.size(); j++) {
                JSONObject itemJSON = bodyJSON.getJSONObject(j);
                if (j == 0) {
                    Set<String> keySet = itemJSON.keySet();
                    columns.addAll(keySet);
                }
                fillData(actions, rows, itemJSON);
            }
        }
    }

    private void execute(List<List> actions, List<String> columns, String tableName) {
        if (!actions.isEmpty()) {
            for (List row : actions) {
                StringBuilder values = new StringBuilder();
                for (Object item : row) {
                    if (item instanceof String) {
                        values.append("'").append(item.toString()).append("',");
                    } else {
                        values.append(item.toString()).append(",");
                    }
                }
                String sql = "";
                try {
                    List<String> columnsReal = new ArrayList<>();
                    sql = prepareSQL(columns, tableName, values, columnsReal);
                    //LOGGER.info("SQL: {}", sql);
                    //stmt.addBatch(sql);
                    stmt.execute(sql);
                    conn.commit();
                } catch (SQLException e) {
                    try {
                        conn.rollback();
                    } catch (SQLException e1) {
                        LOGGER.error("SQL error code: {}", e1.getErrorCode());
                        LOGGER.error("SqlState: {}", e1.getSQLState());
                        LOGGER.error("Error message: {}", e1.getMessage());
                    }
                    LOGGER.error("Error SQL: {}", sql);
                    LOGGER.error("SQL error code: {}", e.getErrorCode());
                    LOGGER.error("SqlState: {}", e.getSQLState());
                    LOGGER.error("Error message: {}", e.getMessage());
                }finally {
                    try {
                        conn.commit();
                    } catch (SQLException e) {
                        LOGGER.error("SQL error code: {}", e.getErrorCode());
                        LOGGER.error("SqlState: {}", e.getSQLState());
                        LOGGER.error("Error message: {}", e.getMessage());
                    }
                }
            }
//            try {
//                stmt.executeBatch();
//                conn.commit();
//            } catch (SQLException e) {
//                LOGGER.error("SQL error code: {}", e.getErrorCode());
//                LOGGER.error("SqlState: {}", e.getSQLState());
//                LOGGER.error("Error message: {}", e.getMessage());
//            }
        }
    }

    private String prepareSQL(List<String> columns, String tableName, StringBuilder values, List<String> columnsReal) {
        String sql;
        // PostgreSQL好像不支持"`"
        for (String col : columns) {
            columnsReal.add("\"" + col + "\"");
        }
        //columnsReal.addAll(columns);
        sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                StringUtils.join(columnsReal.toArray(), ","),
                values.toString().substring(0, values.toString().length() - 1));
        return sql;
    }

    private void clearBatch() {
        try {
            stmt.clearBatch();
        } catch (SQLException e) {
            LOGGER.error("clear batch异常 {}", ExceptionUtils.getFullStackTrace(e));
            System.exit(1);
        }
    }

    private void fillData(List<List> actions, List<Object> rows, JSONObject bodyJSON) {
        Set<Map.Entry<String, Object>> entries = bodyJSON.entrySet();
        for (Map.Entry<String, Object> entry : entries) {
            String key = entry.getKey();
            if (key.equalsIgnoreCase("path")) {
                rows.add(bodyJSON.get(key).toString().replace("\\", "\\\\"));
            } else {
                rows.add(bodyJSON.get(key));
            }
        }
        actions.add(rows);
    }


}
