package org.fluentjdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Row {

    private final ResultSet rs;
    private final Map<String, Integer> columnIndexes = new HashMap<>();

    public Row(ResultSet rs, String tableName) throws SQLException {
        this.rs = rs;

        ResultSetMetaData metaData = rs.getMetaData();
        for (int i=1; i<=metaData.getColumnCount(); i++) {
            if (metaData.getTableName(i).equalsIgnoreCase(tableName)) {
                columnIndexes.put(metaData.getColumnName(i), i);
            }
        }
    }

    public String getString(String fieldName) throws SQLException {
        return rs.getString(fieldName);
    }

    public Long getLong(String fieldName) throws SQLException {
        return rs.getLong(fieldName);
    }

}
