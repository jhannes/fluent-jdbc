package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Generate a statement that will result in either an <code>UPDATE ...</code> or <code>INSERT ...</code> depending
 * on whether the row exists already by collecting field names and parameters. Example:
 *
 * <pre>
 * int count = table
 *      .where("id", id)
 *      .insertOrUpdate()
 *      .setField("name", "Something")
 *      .setField("code", 102)
 *      .execute(connection);
 * </pre>
 */
public class DatabaseInsertOrUpdateBuilder implements DatabaseUpdatable<DatabaseInsertOrUpdateBuilder> {

    private DatabaseUpdateBuilder updateBuilder;
    private DatabaseInsertBuilder insertBuilder;

    public DatabaseInsertOrUpdateBuilder(DatabaseTable table) {
        updateBuilder = new DatabaseUpdateBuilder(table);
        insertBuilder = new DatabaseInsertBuilder(table);
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setFields(List<String> fields, List<?> values) {
        updateBuilder = updateBuilder.setFields(fields, values);
        insertBuilder = insertBuilder.setFields(fields, values);
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setFields(Map<String, ?> fields) {
        updateBuilder = updateBuilder.setFields(fields);
        insertBuilder = insertBuilder.setFields(fields);
        return this;
    }

    /**
     * Adds fieldName to <code>UPDATE ... SET fieldName = ?</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setField(String field, @Nullable Object value) {
        return setField(field, "?", Collections.singleton(value));
    }

    /**
     * Adds fieldName to <code>UPDATE ... SET fieldName = expression</code> and values to the list of parameters
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setField(String field, String expression, Collection<?> values) {
        updateBuilder = updateBuilder.setField(field, expression, values);
        insertBuilder = insertBuilder.setField(field, expression, values);
        return this;
    }

    @CheckReturnValue
    public DatabaseInsertOrUpdateBuilder where(DatabaseWhereBuilder whereClause) {
        updateBuilder = updateBuilder.where(whereClause);
        List<String> columns = whereClause.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            insertBuilder = insertBuilder.setField(column, whereClause.getColumnExpressions().get(i), whereClause.getParameterAsCollection(i));
        }

        return this;
    }

    /**
     * Will generate <code>UPDATE</code> statements, set parameters and execute to database
     * @return returns the number of rows updated or -1 if a row was inserted
     */
    public int execute(Connection connection) {
        int rowCount = updateBuilder.execute(connection);
        if (rowCount != 0) {
            return rowCount;
        }
        insertBuilder.execute(connection);
        return -1;
    }

}
