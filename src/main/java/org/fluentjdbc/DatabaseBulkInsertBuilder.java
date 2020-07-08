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

/**
 * Fluently generate a <code>INSERT ...</code> statement for a list of objects. Crate with a list of object
 * and use {@link #setField(String, Function)} to pass in a function that will be called for each object
 * in the list. Use {@link #generatePrimaryKeys(String, BiConsumer)} to use the return value from
 * the underlying database table autogeneration of keys.
 *
 * <p>Example:</p>
 *
 * <pre>
 *     public void insertAll(List&lt;TagType&gt; tagTypes, Connection connection) {
 *         tagTypesTable.bulkInsert(tagTypes)
 *             .setField("name", TagType::getName)
 *             .generatePrimaryKeys("id", TagType::setId)
 *             .execute(connection);
 *     }
 * </pre>
 */
public class DatabaseBulkInsertBuilder<T> extends DatabaseStatement implements DatabaseBulkUpdatable<T, DatabaseBulkInsertBuilder<T>> {

    private final DatabaseTable table;
    private final Iterable<T> objects;

    private final List<String> updateFields = new ArrayList<>();
    private final List<Function<T, ?>> updateParameters = new ArrayList<>();

    DatabaseBulkInsertBuilder(DatabaseTable table, Iterable<T> objects) {
        this.table = table;
        this.objects = objects;
    }

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk insert
     */
    @Override
    public DatabaseBulkInsertBuilder<T> setField(String fieldName, Function<T, Object> transformer) {
        updateFields.add(fieldName);
        updateParameters.add(transformer);
        return this;
    }

    /**
     * Executes <code>INSERT INTO table ...</code> and calls {@link PreparedStatement#addBatch()} for
     * each row
     *
     * @return the count of rows inserted
     */
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

    /**
     * When called, {@link #execute(Connection)} will use the table autogeneration mechanism
     * to generate primary keys for new rows. For each object in the bulk batch, the specified callback
     * will be executed with the corresponding generated primary key
     */
    public DatabaseBulkInsertBuilderWithPk<T> generatePrimaryKeys(String primaryKeyColumn, BiConsumer<T, Long> consumer) {
        return new DatabaseBulkInsertBuilderWithPk<>(objects, table, updateFields, updateParameters, primaryKeyColumn, consumer);
    }
}
