package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

public class DatabaseBulkDeleteBuilder<T> extends DatabaseStatement
        implements DatabaseBulkQueriable<T, DatabaseBulkDeleteBuilder<T>> {

    private final List<String> whereConditions = new ArrayList<>();
    private final List<Function<T, ?>> whereParameters = new ArrayList<>();
    private final DatabaseTableImpl table;
    private final Iterable<T> objects;

    public DatabaseBulkDeleteBuilder(DatabaseTableImpl table, Iterable<T> objects) {
        this.table = table;
        this.objects = objects;
    }

    @Override
    public DatabaseBulkDeleteBuilder<T> where(String field, Function<T, ?> value) {
        whereConditions.add(field + " = ?");
        whereParameters.add(value);
        return this;
    }

    public int execute(Connection connection) {
        String deleteStatement = createDeleteStatement(table.getTableName(), whereConditions);
        try (PreparedStatement statement = connection.prepareStatement(deleteStatement)) {
            addBatch(statement, objects, whereParameters);
            int[] counts = statement.executeBatch();
            return IntStream.of(counts).sum();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }
}
