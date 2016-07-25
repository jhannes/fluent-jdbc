package org.fluentjdbc;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class DatabaseRow {

    private final static Logger logger = LoggerFactory.getLogger(DatabaseRow.class);

    private final ResultSet rs;
    private final Map<String, Integer> columnIndexes = new HashMap<>();

    DatabaseRow(ResultSet rs, String tableName) throws SQLException {
        this.rs = rs;

        ResultSetMetaData metaData = rs.getMetaData();
        for (int i=1; i<=metaData.getColumnCount(); i++) {
            // TODO: This doesn't work on Android or SQL server
            if (metaData.getTableName(i).isEmpty()) {
                throw new IllegalStateException("getTableName not supported");
            }
            if (metaData.getTableName(i).equalsIgnoreCase(tableName)) {
                columnIndexes.put(metaData.getColumnName(i).toUpperCase(), i);
            }
        }
    }

    public DatabaseRow(ResultSet rs) throws SQLException {
        this.rs = rs;

        ResultSetMetaData metaData = rs.getMetaData();
        for (int i=1; i<=metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i).toUpperCase();
            if (!columnIndexes.containsKey(columnName)) {
                columnIndexes.put(columnName, i);
            } else {
                logger.warn("Duplicate column " + columnName + " in query result");
            }
        }
    }

    public DateTime getDateTime(String fieldName) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(getColumnIndex(fieldName));
        return timestamp != null ? new DateTime(timestamp.getTime()) : null;
    }

    public String getString(String fieldName) throws SQLException {
        return rs.getString(getColumnIndex(fieldName));
    }

    public Long getLong(String fieldName) throws SQLException {
        return rs.getLong(getColumnIndex(fieldName));
    }

    public Object getObject(String fieldName) throws SQLException {
        return rs.getObject(getColumnIndex(fieldName));
    }

    private Integer getColumnIndex(String fieldName) {
        if (!columnIndexes.containsKey(fieldName.toUpperCase())) {
            throw new IllegalArgumentException("Column {" + fieldName + "} is not present in " + columnIndexes.keySet());
        }
        return columnIndexes.get(fieldName.toUpperCase());
    }

}
