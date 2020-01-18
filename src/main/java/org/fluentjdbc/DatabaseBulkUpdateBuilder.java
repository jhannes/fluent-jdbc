package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

public class DatabaseBulkUpdateBuilder<T> extends DatabaseStatement
        implements DatabaseBulkQueriable<T, DatabaseBulkUpdateBuilder<T>>, DatabaseBulkUpdateable<T, DatabaseBulkUpdateBuilder<T>> {

    private final DatabaseTable table;
    private final List<T> objects;

    private final List<String> whereConditions = new ArrayList<>();
    private final List<Function<T, ?>> whereParameters = new ArrayList<>();

    private final List<String> updateFields = new ArrayList<>();
    private final List<Function<T, ?>> updateParameters = new ArrayList<>();

    public DatabaseBulkUpdateBuilder(DatabaseTable table, List<T> objects) {
        this.table = table;
        this.objects = objects;
    }

    @Override
    public DatabaseBulkUpdateBuilder<T> where(String field, Function<T, ?> value) {
        whereConditions.add(field + " = ?");
        whereParameters.add(value);
        return this;
    }

    @Override
    public DatabaseBulkUpdateBuilder<T> setField(String fieldName, Function<T, Object> transformer) {
        updateFields.add(fieldName);
        updateParameters.add(transformer);
        return this;
    }

    public int execute(Connection connection) {
        String updateStatement = createUpdateStatement(table.getTableName(), updateFields, whereConditions);
        try (PreparedStatement statement = connection.prepareStatement(updateStatement)) {
            List<Function<T, ?>> parameters = new ArrayList<>();
            parameters.addAll(updateParameters);
            parameters.addAll(whereParameters);
            addBatch(statement, objects, parameters);
            int[] counts = statement.executeBatch();
            return IntStream.of(counts).sum();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }
}
