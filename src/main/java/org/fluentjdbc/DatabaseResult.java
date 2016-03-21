package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseResult implements AutoCloseable {

    private ResultSet resultSet;
    private Map<String, DatabaseRow> tableRows = new HashMap<>();

    public DatabaseResult(PreparedStatement stmt) throws SQLException {
        resultSet = stmt.executeQuery();
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
    }

    public boolean next() throws SQLException {
        return resultSet.next();
    }

    public DatabaseRow table(String tableName) throws SQLException {
        return tableRows.computeIfAbsent(tableName, key -> new DatabaseRow(resultSet, key));
    }

    public <T> List<T> list(String tableName, RowMapper<T> mapper) throws SQLException {
        List<T> result = new ArrayList<>();
        while (next()) {
            result.add(mapper.mapRow(table(tableName)));
        }
        return result;
    }

    public <T> T single(String tableName, RowMapper<T> mapper) throws SQLException {
        if (!next()) {
            return null;
        }
        T result = mapper.mapRow(table(tableName));
        if (next()) {
            throw new RuntimeException("More than one row returned");
        }
        return result;
    }

}
