package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import java.sql.Connection;
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
     * Adds parameters to the <code>INSERT</code> and <code>UPDATE</code> statements and list of parameters
     */
    public DatabaseInsertOrUpdateBuilder addParameters(List<DatabaseQueryParameter> queryParameters) {
        updateBuilder = updateBuilder.addParameters(queryParameters);
        insertBuilder = insertBuilder.addParameters(queryParameters);
        return this;
    }

    /**
     * Adds a parameter to the <code>INSERT</code> and <code>UPDATE</code> statements and list of parameters
     */
    @CheckReturnValue
    public DatabaseInsertOrUpdateBuilder addParameter(DatabaseQueryParameter parameter) {
        updateBuilder = updateBuilder.addParameter(parameter);
        insertBuilder = insertBuilder.addParameter(parameter);
        return this;
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

    @CheckReturnValue
    public DatabaseInsertOrUpdateBuilder where(DatabaseWhereBuilder whereClause) {
        updateBuilder = updateBuilder.where(whereClause);
        //noinspection ResultOfMethodCallIgnored
        insertBuilder.addParameters(whereClause.getQueryParameters());
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
