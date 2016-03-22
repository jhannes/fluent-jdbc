package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseTableWithTimestamps extends DatabaseStatement implements DatabaseTable {

    private String tableName;

    public DatabaseTableWithTimestamps(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public DatabaseSaveBuilder newSaveBuilder(String idField, @Nullable Number id) {
        return new DatabaseSaveBuilder(this, idField, id);
    }

    @Override
    public DatabaseQueryBuilder where(String fieldName, @Nullable Object value) {
        return new DatabaseQueryBuilder(this).where(fieldName, value);
    }

    @Override
    public DatabaseQueryBuilder whereExpression(String expression, Object parameter) {
        return new DatabaseQueryBuilder(this).whereExpression(expression, parameter);
    }

    @Override
    public <T> List<T> listObjects(Connection connection, RowMapper<T> mapper) {
        logger.debug("select * from " + tableName);
        try(PreparedStatement stmt = connection.prepareStatement("select * from " + tableName)) {
            try (DatabaseResult result = new DatabaseResult(stmt)) {
                return result.list(tableName, mapper);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    @Override
    public DatabaseQueryBuilder whereAll(List<String> fieldNames, List<Object> values) {
        return new DatabaseQueryBuilder(this).whereAll(fieldNames, values);
    }

    @Override
    public DatabaseInsertBuilder insert() {
        return new DatabaseInsertBuilder(this);
    }


}
