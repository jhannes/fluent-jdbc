package org.fluentjdbc;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class DatabaseRow {

    private final Map<String, Integer> columnIndexes;
    private final Map<String, Map<String, Integer>> tableColumnIndexes;
    private final Map<DatabaseTableAlias, Integer> keys;
    protected final ResultSet rs;

    protected DatabaseRow(ResultSet rs, Map<String, Integer> columnIndexes, Map<String, Map<String, Integer>> tableColumnIndexes, Map<DatabaseTableAlias, Integer> keys) {
        this.rs = rs;
        this.columnIndexes = columnIndexes;
        this.tableColumnIndexes = tableColumnIndexes;
        this.keys = keys;
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

    public boolean getBoolean(String column) throws SQLException {
        return rs.getBoolean(getColumnIndex(column));
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

    public LocalDate getLocalDate(String column) throws SQLException {
        Date date = rs.getDate(getColumnIndex(column));
        return date != null ? date.toLocalDate() : null;
    }

    public UUID getUUID(String fieldName) throws SQLException {
        String result = getString(fieldName);
        return result != null ? UUID.fromString(result) : null;
    }

    public Double getDouble(String column) throws SQLException {
        double result = rs.getDouble(getColumnIndex(column));
        return !rs.wasNull() ? result : null;
    }

    public <T extends Enum<T>> T getEnum(Class<T> enumClass, String fieldName) throws SQLException {
        String value = getString(fieldName);
        return value != null ? Enum.valueOf(enumClass, value) : null;
    }

    protected Integer getColumnIndex(String fieldName) {
        if (!columnIndexes.containsKey(fieldName.toUpperCase())) {
            throw new IllegalArgumentException("Column {" + fieldName + "} is not present in " + columnIndexes.keySet());
        }
        return columnIndexes.get(fieldName.toUpperCase());
    }

    public <T> Optional<T> table(DatabaseTableAlias alias, DatabaseTable.RowMapper<T> function) throws SQLException {
        DatabaseRow table = table(alias);
        return table != null ? Optional.of(function.mapRow(table)) : Optional.empty();
    }

    public DatabaseRow table(DatabaseTableAlias alias) throws SQLException {
        if (keys.containsKey(alias) && rs.getObject(keys.get(alias)) == null) {
            return null;
        }
        return table(alias.getAlias());
    }

    public DatabaseRow table(DbTableAliasContext alias) throws SQLException {
        return table(alias.getTableAlias());
    }

    public DatabaseRow table(String table) {
        Map<String, Integer> columnIndexes = tableColumnIndexes.get(table.toUpperCase());
        if (columnIndexes == null) {
            throw new IllegalArgumentException("Unknown table " + table.toUpperCase() + " in " + tableColumnIndexes.keySet());
        }
        return new DatabaseRow(rs, tableColumnIndexes.get(table.toUpperCase()), tableColumnIndexes, this.keys);
    }
}
