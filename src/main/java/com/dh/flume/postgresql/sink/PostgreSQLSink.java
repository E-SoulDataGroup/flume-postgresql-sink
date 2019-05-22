package com.dh.flume.postgresql.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dh.flume.postgresql.util.DBUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * PostgreSQLSink
 *
 * @author GeZhiHui
 * @create 2019-05-22
 **/

public class PostgreSQLSink extends AbstractSink implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSink.class);

    private String hostname;
    private String port;
    private String database;
    private String user;
    private String password;
    private Integer batchSize;

    private String topic;

    private Map<String, Map<String, String>> topicToTables = new HashMap<>();
    private List<String> tableList = new ArrayList<>();
    private Map<String, List<String>> tableColumns = new HashMap<>();
    private Map<String, List<String>> tableColumnTypes = new HashMap<>();
    private Map<String, Map<String, List<String>>> tableUniqueKeys = new HashMap<>();
    private Map<String, String> tableWriteModes = new HashMap<>();
    private Map<String, String> sqls = new HashMap<>();
    private Map<String, PreparedStatement> stmts = new HashMap<>();

    private Connection conn;
    private PostgresqlDatabaseMeta pgm;

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
            for (String tableName : tableList) {
                List<String> columns = DBUtil.getColumns(tableName, conn);
                List<String> columnTypes = DBUtil.getColumnTypes(tableName, conn);
                tableColumns.put(tableName, columns);
                tableColumnTypes.put(tableName, columnTypes);
                Map<String, List<String>> primaryOrUniqueKeys = DBUtil.getPrimaryOrUniqueKeys(tableName, conn);
                tableUniqueKeys.put(tableName, primaryOrUniqueKeys);
                List<String> tableColumn = tableColumns.get(tableName);
                String writeMode = tableWriteModes.get(tableName);
                if (StringUtils.equalsIgnoreCase(writeMode, WriteMode.INSERT.name())) {
                    String sql = pgm.getInsertStatement(tableColumn, tableName);
                    sqls.put(tableName, sql);
                    stmts.put(tableName, conn.prepareStatement(sql));
                } else {
                    String sql = pgm.getReplaceStatement(tableColumn, tableName, primaryOrUniqueKeys);
                    sqls.put(tableName, sql);
                    stmts.put(tableName, conn.prepareStatement(sql));
                }
            }

        } catch (SQLException e) {
            LOGGER.error("获取pg连接失败 --> {}", ExceptionUtils.getFullStackTrace(e));
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        DBUtil.closeDBResources(null, stmts.values(), conn);
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
        topic = context.getString("topic", "topic");
        batchSize = context.getInteger("batchSize", 100);
        String topicToTable = context.getString("topicToTable");
        JSONArray jsonArray = JSONObject.parseArray(topicToTable);
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            Map<String, String> map = new HashMap<>();
            String tableName = jsonObject.getString("tableName");
            String writeMode = jsonObject.getString("writeMode");
            String topicName = jsonObject.getString("topicName");
            map.put("topicName", topicName);
            map.put("tableName", tableName);
            map.put("writeMode", writeMode);
            topicToTables.put(topicName, map);
            tableList.add(tableName);
            tableWriteModes.put(tableName, writeMode);
        }
        pgm = new PostgresqlDatabaseMeta();
    }

    @Override
    public Status process() {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event;
        String body;
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            clearBatch();
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    Map<String, String> headers = event.getHeaders();
                    String topicName = headers.get(topic);
                    String tableName = topicToTables.get(topicName).get("tableName");
                    List<String> columns = tableColumns.get(tableName);
                    List<String> columnTypes = tableColumnTypes.get(tableName);
                    body = new String(event.getBody());
                    JSONObject jsonObject = JSONObject.parseObject(body);
                    for (int j = 0; j < columns.size(); j++) {
                        //TODO stream表额外增加的字段
                        if (StringUtils.equalsIgnoreCase(columns.get(j), "arrival_timestamp")) {
                            stmts.get(tableName).setObject(j + 1, jsonObject.get(columns.get(j)));
                        } else {
                            String columnType = columnTypes.get(j);
                            Object convert = PostgresqlTypeConverter.convert(jsonObject.get(columns.get(j)), columnType);
                            DBUtil.setParameterValue(convert, stmts.get(tableName), j);
                        }
                    }
                    stmts.get(tableName).addBatch();
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            executeBatch();
            conn.commit();
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
            try {
                if (conn != null) {
                    conn.commit();
                }
            } catch (SQLException e) {
                LOGGER.error("SQL error code: {}", e.getErrorCode());
                LOGGER.error("SqlState: {}", e.getSQLState());
                LOGGER.error("Error message: {}", e.getMessage());
            }
        }

        return result;
    }

    private void executeBatch() throws SQLException {
        for (PreparedStatement statement : stmts.values()) {
            statement.executeBatch();
        }
    }

    private void clearBatch() {
        try {
            for (PreparedStatement statement : stmts.values()) {
                statement.clearBatch();
            }
        } catch (SQLException e) {
            LOGGER.error(ExceptionUtils.getFullStackTrace(e));
            System.exit(1);
        }
    }


}
