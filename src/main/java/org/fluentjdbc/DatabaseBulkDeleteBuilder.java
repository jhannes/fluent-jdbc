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

/**
 * Fluently generate a <code>DELETE ... WHERE ...</code> statement for a list of objects.
 * Create with a list of object and use {@link #where(String, Function)} to add a function
 * which will be called for each object to create the <code>WHERE field = ?</code> value.
 *
 * <p>Example:</p>
 *
 * <pre>
 *     public void deleteAll(List&lt;TagType&gt; tagTypes, Connection connection) {
 *         tagTypesTable.bulkDelete(tagTypes)
 *              .where("id", TagType::getId)
 *              .execute(connection);
 *     }
 * </pre>
 */
public class DatabaseBulkDeleteBuilder<T> implements DatabaseBulkQueryable<T, DatabaseBulkDeleteBuilder<T>> {

    private final List<String> whereConditions = new ArrayList<>();
    private final List<Function<T, ?>> whereParameters = new ArrayList<>();
    private final DatabaseTable table;
    private final Iterable<T> objects;

    public DatabaseBulkDeleteBuilder(DatabaseTable table, Iterable<T> objects) {
        this.table = table;
        this.objects = objects;
    }

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk update
     * to extract the values for the <code>WHERE fieldName = ?</code> clause
     */
    @Override
    public DatabaseBulkDeleteBuilder<T> where(String field, Function<T, ?> value) {
        whereConditions.add(field + " = ?");
        whereParameters.add(value);
        return this;
    }

    /**
     * Executes <code>DELETE FROM table WHERE field = ? AND ...</code>
     * and calls {@link PreparedStatement#addBatch()} for each row
     *
     * @return the sum count of all the rows deleted
     */
    public int execute(Connection connection) {
        String deleteStatement = "delete from " + table.getTableName() + " where " + String.join(" and ", whereConditions);
        try (PreparedStatement statement = connection.prepareStatement(deleteStatement)) {
            addBatch(statement, objects, whereParameters);
            int[] counts = statement.executeBatch();
            return IntStream.of(counts).sum();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }
}
