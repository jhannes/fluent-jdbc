package org.fluentjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DatabaseRow {

    private final static Logger logger = LoggerFactory.getLogger(DatabaseRow.class);

    private final ResultSet rs;
    private final Map<String, Integer> columnIndexes = new HashMap<>();
    private Map<DatabaseColumnReference, Integer> columnMap;

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

    public DatabaseRow(ResultSet rs, Map<DatabaseColumnReference, Integer> columnMap) {
        this.rs = rs;
        this.columnMap = columnMap;
    }

    public Instant getInstant(String fieldName) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(getColumnIndex(fieldName));
        return timestamp != null ? timestamp.toInstant() : null;
    }

    public ZonedDateTime getZonedDateTime(String fieldName) throws SQLException {
        Instant instant = getInstant(fieldName);
        return instant != null ? instant.atZone(ZoneId.systemDefault()) : null;
    }

    public String getString(String fieldName) throws SQLException {
        return rs.getString(getColumnIndex(fieldName));
    }

    public Long getLong(String fieldName) throws SQLException {
        long result = rs.getLong(getColumnIndex(fieldName));
        return rs.wasNull() ? null : result;
    }

    public Object getObject(String fieldName) throws SQLException {
        return rs.getObject(getColumnIndex(fieldName));
    }

    public UUID getUUID(String fieldName) throws SQLException {
        String result = getString(fieldName);
        return result != null ? UUID.fromString(result) : null;
    }

    public boolean getBoolean(String fieldName) throws SQLException {
        return  rs.getBoolean(fieldName);
    }

    private Integer getColumnIndex(String fieldName) {
        if (!columnIndexes.containsKey(fieldName.toUpperCase())) {
            throw new IllegalArgumentException("Column {" + fieldName + "} is not present in " + columnIndexes.keySet());
        }
        return columnIndexes.get(fieldName.toUpperCase());
    }

    public LocalDate getLocalDate(String fieldName) throws SQLException {
        Date date = rs.getDate(fieldName);
        return date != null ? date.toLocalDate() : null;
    }

    public <T extends Enum<T>> T getEnum(Class<T> enumClass, String fieldName) throws SQLException {
        String value = getString(fieldName);
        return value != null ? Enum.valueOf(enumClass, value) : null;
    }

    public Long getLong(DatabaseColumnReference column) throws SQLException {
        long result = rs.getLong(columnMap.get(column));
        return rs.wasNull() ? null : result;
    }

    public String getString(DatabaseColumnReference column) throws SQLException {
        return rs.getString(columnMap.get(column));
    }

}
