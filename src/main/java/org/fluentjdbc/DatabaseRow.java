package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DatabaseRow {

    private final ResultSet rs;
    private final Map<String, Integer> columnIndexes = new HashMap<>();
    private String tableName;

    DatabaseRow(ResultSet rs, String tableName) {
        this.rs = rs;
        this.tableName = tableName;

        try {
            ResultSetMetaData metaData = rs.getMetaData();
            for (int i=1; i<=metaData.getColumnCount(); i++) {
                if (metaData.getTableName(i).equalsIgnoreCase(tableName)) {
                    columnIndexes.put(metaData.getColumnName(i).toUpperCase(), i);
                }
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public String getString(String fieldName) throws SQLException {
        return rs.getString(getColumnIndex(fieldName));
    }

    public Long getLong(String fieldName) throws SQLException {
        return rs.getLong(getColumnIndex(fieldName));
    }


    private Integer getColumnIndex(String fieldName) {
        if (!columnIndexes.containsKey(fieldName.toUpperCase())) {
            throw new IllegalArgumentException("Column {" + fieldName + "} is not present in {" + tableName + "}: " + columnIndexes.keySet());
        }
        return columnIndexes.get(fieldName.toUpperCase());
    }
}
