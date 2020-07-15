package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.fluentjdbc.DatabaseStatement.addBatch;
import static org.fluentjdbc.DatabaseStatement.createUpdateStatement;

/**
 * Fluently generate a <code>UPDATE ...</code> statement for a list of objects. Create with a list of object
 * and use {@link #setField(String, Function)} to pass in a function that will be called for each object
 * in the list to create the <code>SET field = ?</code> value and {@link #where(String, Function)}
 * which will be called for each object to create the <code>WHERE field = ?</code> value.
 *
 * <p>Example:</p>
 *
 * <pre>
 *     public void updateAll(List&lt;TagType&gt; tagTypes, Connection connection) {
 *         tagTypesTable.bulkUpdate(tagTypes)
 *              .where("id", TagType::getId)
 *              .setField("name", TagType::getName)
 *              .execute(connection);
 *     }
 * </pre>
 */
public class DatabaseBulkUpdateBuilder<T> implements
        DatabaseBulkQueryable<T, DatabaseBulkUpdateBuilder<T>>,
        DatabaseBulkUpdatable<T, DatabaseBulkUpdateBuilder<T>>
{

    private final DatabaseTable table;
    private final Iterable<T> objects;

    private final List<String> whereConditions = new ArrayList<>();
    private final List<Function<T, ?>> whereParameters = new ArrayList<>();

    private final List<String> updateFields = new ArrayList<>();
    private final List<Function<T, ?>> updateParameters = new ArrayList<>();

    public DatabaseBulkUpdateBuilder(DatabaseTable table, Iterable<T> objects) {
        this.table = table;
        this.objects = objects;
    }

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk update
     * to extract the values for the <code>WHERE fieldName = ?</code> clause
     */
    @Override
    public DatabaseBulkUpdateBuilder<T> where(String field, Function<T, ?> value) {
        whereConditions.add(field + " = ?");
        whereParameters.add(value);
        return this;
    }

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk update
     * to extract the values for the <code>SET fieldName = ?</code> clause
     */
    @Override
    public DatabaseBulkUpdateBuilder<T> setField(String fieldName, Function<T, Object> transformer) {
        updateFields.add(fieldName);
        updateParameters.add(transformer);
        return this;
    }

    /**
     * Executes <code>UPDATE table SET field = ?, ... WHERE field = ? AND ...</code>
     * and calls {@link PreparedStatement#addBatch()} for each row
     *
     * @return the sum count of all the rows updated
     */
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
