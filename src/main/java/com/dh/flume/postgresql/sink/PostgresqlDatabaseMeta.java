package com.dh.flume.postgresql.sink;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * PostgresqlDatabaseMeta
 *
 * @author GeZhiHui
 * @create 2019-05-20
 **/

public class PostgresqlDatabaseMeta {

    protected String makeMultipleValues(int nCols, int batchSize) {
        String value = makeValues(nCols);
        return StringUtils.repeat(value, ",", batchSize);
    }

    protected String makeValues(int nCols) {
        return "(" + StringUtils.repeat("?", ",", nCols) + ")";
    }

    private String makeUpdatePart(List<String> column) {
        List<String> updateList = new ArrayList<>();
        for (String col : column) {
            String quotedCol = quoteColumn(col);
            updateList.add(quotedCol + "=EXCLUDED." + quotedCol);
        }
        return StringUtils.join(updateList, ",");
    }

    private String makeUpdateKey(Map<String, List<String>> updateKey) {
        Iterator<Map.Entry<String, List<String>>> it = updateKey.entrySet().iterator();
        return StringUtils.join(it.next().getValue(), ",");
    }

    public String getInsertStatement(List<String> column, String table) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values "
                + makeValues(column.size());
    }

    public String getMultiInsertStatement(List<String> column, String table, int batchSize) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values "
                + makeMultipleValues(column.size(), batchSize);
    }

    public String getReplaceStatement(List<String> column, String table, Map<String, List<String>> updateKey) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") VALUES "
                + makeValues(column.size())
                + "ON CONFLICT(" + makeUpdateKey(updateKey)
                + ") DO UPDATE SET " + makeUpdatePart(column);
    }

    public String getMultiReplaceStatement(List<String> column, String table, int batchSize, Map<String, List<String>> updateKey) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") VALUES "
                + makeMultipleValues(column.size(), batchSize)
                + "ON CONFLICT(" + makeUpdateKey(updateKey)
                + ") DO UPDATE SET " + makeUpdatePart(column);
    }

    public String getStartQuote() {
        return "";
    }

    public String getEndQuote() {
        return "";
    }

    public String quoteColumn(String column) {
        return getStartQuote() + column + getEndQuote();
    }

    public String quoteColumns(List<String> column) {
        return quoteColumns(column, null);
    }

    public String quoteColumns(List<String> column, String table) {
        String prefix = org.apache.commons.lang.StringUtils.isBlank(table) ? "" : quoteTable(table) + ".";
        List<String> list = new ArrayList<>();
        for (String col : column) {
            list.add(prefix + quoteColumn(col));
        }
        return org.apache.commons.lang.StringUtils.join(list, ",");
    }

    public String quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if (i != 0) {
                sb.append(".");
            }
            sb.append(getStartQuote() + parts[i] + getEndQuote());
        }
        return sb.toString();
    }

}
