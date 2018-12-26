package com.dh.flume.postgresql.test;

import com.dh.flume.postgresql.util.DBUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Test
 *
 * @author GeZhiHui
 * @create 2018-12-21
 **/

public class Test {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test.class);

    private static final String URL = "jdbc:postgresql://192.168.112.42:5432/dvdrental";
    private static final String USER = "postgres";
    private static final String PASSWORD = "123456";

    public static void main(String[] args) {
        insert();
        executeQuery();
    }

    private static void insert() {
        String sql = "INSERT INTO actor(first_name,last_name) VALUES(?,?);";
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = DBUtil.getConnection(URL, USER, PASSWORD);
            statement = connection.prepareStatement(sql);
            statement.setString(1, "ma");
            statement.setString(2, "long");
            statement.addBatch();
            int[] ints = statement.executeBatch();
            System.out.println(ints);
        } catch (SQLException e) {
            LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        } finally {
            DBUtil.closeDBResources(null, statement, connection);
        }
    }

    private static void executeQuery() {
        String sql = "SELECT actor_id, first_name, last_name, last_update FROM public.actor;";
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DBUtil.getConnection(URL, USER, PASSWORD);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 0; i < columnCount; i++) {
                    Object object = resultSet.getObject(i + 1);
                    System.out.println(object);
                }
                System.out.println("--------------------");
            }
        } catch (SQLException e) {
            LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        } finally {
            DBUtil.closeDBResources(resultSet, statement, connection);
        }
    }


}
