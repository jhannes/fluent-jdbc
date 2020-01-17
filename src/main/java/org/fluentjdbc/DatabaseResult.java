package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseResult implements AutoCloseable {

    private ResultSet resultSet;
    private Map<String, DatabaseRow> tableRows = new HashMap<>();

    public DatabaseResult(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
    }

    public boolean next() throws SQLException {
        return resultSet.next();
    }

    // TODO: This doesn't work on Android or SQL server
    public DatabaseRow table(String tableName) throws SQLException {
        if (!tableRows.containsKey(tableName)) {
            tableRows.put(tableName, new DatabaseRow(resultSet, tableName));
        }
        return tableRows.get(tableName);
    }

    public <T> List<T> list(RowMapper<T> mapper) throws SQLException {
        List<T> result = new ArrayList<>();
        while (next()) {
            result.add(mapper.mapRow(createDatabaseRow(resultSet)));
        }
        return result;
    }

    public void forEach(DatabaseTable.RowConsumer consumer) throws SQLException {
        while (next()) {
            consumer.apply(createDatabaseRow(resultSet));
        }
    }

    @Nullable
    public <T> T single(RowMapper<T> mapper) throws SQLException {
        if (!next()) {
            return null;
        }
        T result = mapper.mapRow(createDatabaseRow(resultSet));
        if (next()) {
            throw new IllegalStateException("More than one row returned");
        }
        return result;
    }

    protected DatabaseRow createDatabaseRow(ResultSet resultSet) throws SQLException {
        return new DatabaseRow(this.resultSet);
    }

}
