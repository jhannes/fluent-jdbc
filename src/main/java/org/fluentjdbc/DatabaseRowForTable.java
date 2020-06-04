package org.fluentjdbc;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Map;

class DatabaseRowForTable extends DatabaseRow {
    private final DatabaseTableAlias alias;

    public DatabaseRowForTable(DatabaseTableAlias alias, ResultSet rs, Map<DatabaseColumnReference, Integer> columnMap) throws SQLException {
        super(rs, columnMap);
        this.alias = alias;
    }

    @Override
    public Object getObject(String column) throws SQLException {
        return rs.getObject(getColumnIndex(column));
    }

    @Override
    public String getString(String column) throws SQLException {
        return rs.getString(getColumnIndex(column));
    }

    @Override
    public Long getLong(String column) throws SQLException {
        return rs.getLong(getColumnIndex(column));
    }

    @Override
    public Integer getInt(String column) throws SQLException {
        return rs.getInt(getColumnIndex(column));
    }

    @Override
    public boolean getBoolean(String fieldName) throws SQLException {
        return rs.getBoolean(getColumnIndex(fieldName));
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws SQLException {
        return rs.getTimestamp(getColumnIndex(fieldName));
    }

    @Override
    public LocalDate getLocalDate(String fieldName) throws SQLException {
        Date date = rs.getDate(getColumnIndex(fieldName));
        return date != null ? date.toLocalDate() : null;
    }

    @Override
    public Double getDouble(String fieldName) throws SQLException {
        return rs.getDouble(getColumnIndex(fieldName));
    }

    private Integer getColumnIndex(String fieldName) {
        DatabaseColumnReference column = alias.column(fieldName);
        if (!columnMap.containsKey(column)) {
            throw new IllegalArgumentException("Column {" + column + "} is not present in " + columnMap.keySet());
        }
        return columnMap.get(column);
    }

}
