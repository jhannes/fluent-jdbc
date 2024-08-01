package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.LinkedHashMap;
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

    @Nonnull
    private final DatabaseTable table;
    private final Map<String, Object> updateFields = new LinkedHashMap<>();
    private DatabaseWhereBuilder whereClause;

    public DatabaseInsertOrUpdateBuilder(DatabaseTable table) {
        this.table = table;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setFields(List<String> fields, List<?> values) {
        for (int i = 0; i < fields.size(); i++) {
            updateFields.put(fields.get(i), values.get(i));
        }
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setFields(Map<String, ?> fields) {
        updateFields.putAll(fields);
        return this;
    }

    /**
     * Adds fieldName to <code>UPDATE ... SET fieldName = ?</code> and parameter to the list of parameters
     */
    @Override
    public DatabaseInsertOrUpdateBuilder setField(String field, @Nullable Object value) {
        updateFields.put(field, value);
        return this;
    }

    @CheckReturnValue
    public DatabaseInsertOrUpdateBuilder where(DatabaseWhereBuilder whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    /**
     * Will generate <code>UPDATE</code> statements, set parameters and execute to database
     * @return returns the number of rows updated or -1 if a row was inserted
     */
    public int execute(Connection connection) {
        int rowCount = new DatabaseUpdateBuilder(table).where(whereClause).setFields(updateFields)
                .execute(connection);
        if (rowCount != 0) {
            return rowCount;
        }
        new DatabaseInsertBuilder(table)
                .setFields(whereClause.getColumns(), whereClause.getParameters())
                .setFields(updateFields)
                .execute(connection);
        return -1;
    }

}
