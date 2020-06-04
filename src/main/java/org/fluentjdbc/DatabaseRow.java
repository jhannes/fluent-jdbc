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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DatabaseRow {

    private final static Logger logger = LoggerFactory.getLogger(DatabaseRow.class);

    protected final ResultSet rs;
    private final Map<String, Integer> columnIndexes = new HashMap<>();
    protected Map<DatabaseColumnReference, Integer> columnMap;

    DatabaseRow(ResultSet rs, String tableName) throws SQLException {
        this.rs = rs;

        ResultSetMetaData metaData = rs.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
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
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
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

    public Object getObject(String column) throws SQLException {
        return rs.getObject(getColumnIndex(column));
    }

    public String getString(String column) throws SQLException {
        return rs.getString(getColumnIndex(column));
    }

    public Long getLong(String column) throws SQLException {
        long result = rs.getLong(getColumnIndex(column));
        return rs.wasNull() ? null : result;
    }

    public Integer getInt(String column) throws SQLException {
        int result = rs.getInt(getColumnIndex(column));
        return rs.wasNull() ? null : result;
    }

    public boolean getBoolean(String fieldName) throws SQLException {
        return rs.getBoolean(fieldName);
    }

    public Timestamp getTimestamp(String column) throws SQLException {
        return rs.getTimestamp(getColumnIndex(column));
    }

    public Instant getInstant(String column) throws SQLException {
        Timestamp timestamp = getTimestamp(column);
        return timestamp != null ? timestamp.toInstant() : null;
    }

    public ZonedDateTime getZonedDateTime(String fieldName) throws SQLException {
        Instant instant = getInstant(fieldName);
        return instant != null ? instant.atZone(ZoneId.systemDefault()) : null;
    }

    public OffsetDateTime getOffsetDateTime(String fieldName) throws SQLException {
        Instant instant = getInstant(fieldName);
        return instant != null ? OffsetDateTime.ofInstant(instant, ZoneId.systemDefault()) : null;
    }

    public LocalDate getLocalDate(String fieldName) throws SQLException {
        Date date = rs.getDate(fieldName);
        return date != null ? date.toLocalDate() : null;
    }

    public UUID getUUID(String fieldName) throws SQLException {
        String result = getString(fieldName);
        return result != null ? UUID.fromString(result) : null;
    }

    public Double getDouble(String fieldName) throws SQLException {
        double result = rs.getDouble(fieldName);
        return !rs.wasNull() ? result : null;
    }

    public <T extends Enum<T>> T getEnum(Class<T> enumClass, String fieldName) throws SQLException {
        String value = getString(fieldName);
        return value != null ? Enum.valueOf(enumClass, value) : null;
    }

    private Integer getColumnIndex(String fieldName) {
        if (!columnIndexes.containsKey(fieldName.toUpperCase())) {
            throw new IllegalArgumentException("Column {" + fieldName + "} is not present in " + columnIndexes.keySet());
        }
        return columnIndexes.get(fieldName.toUpperCase());
    }

    public DatabaseRow table(DatabaseTableAlias alias) {
        return new DatabaseRowForTable(alias, rs, columnMap);
    }

    public DatabaseRow table(DbTableAliasContext alias) {
        return table(alias.getTableAlias());
    }

}
