package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;


public class DatabaseBulkInsertBuilder<T> extends DatabaseStatement implements DatabaseBulkUpdateable<T, DatabaseBulkInsertBuilder<T>> {

    private final DatabaseTable table;
    private final Iterable<T> objects;

    private final List<String> updateFields = new ArrayList<>();
    private final List<Function<T, ?>> updateParameters = new ArrayList<>();

    DatabaseBulkInsertBuilder(DatabaseTable table, Iterable<T> objects) {
        this.table = table;
        this.objects = objects;
    }

    @Override
    public DatabaseBulkInsertBuilder<T> setField(String fieldName, Function<T, Object> transformer) {
        updateFields.add(fieldName);
        updateParameters.add(transformer);
        return this;
    }

    public int execute(Connection connection) {
        String insertStatement = createInsertSql(table.getTableName(), updateFields);
        try (PreparedStatement statement = connection.prepareStatement(insertStatement)) {
            addBatch(statement, objects, updateParameters);
            int[] counts = statement.executeBatch();
            return IntStream.of(counts).sum();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public DatabaseBulkInsertBuilderWithPk<T> generatePrimaryKeys(BiConsumer<T, Long> consumer) {
        return new DatabaseBulkInsertBuilderWithPk<>(objects, table, updateFields, updateParameters, consumer);
    }
}
