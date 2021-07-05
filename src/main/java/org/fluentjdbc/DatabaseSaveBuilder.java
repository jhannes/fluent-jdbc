package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Generate <code>INSERT</code> or <code>UPDATE</code> statements on a {@link DatabaseTable} by
 * collecting field names and parameters. Support autogeneration of primary keys and unique
 * natural keys. Example:
 *
 * <pre>
 * DatabaseSaveResult&lt;Long&gt; result = table.newSaveBuilder("id", object.getId())
 *      .uniqueKey("name", object.getName())
 *      .setField("value", object.getValue())
 *      .execute(connection);
 * object.setId(result.getId());
 * </pre>
 */
@ParametersAreNonnullByDefault
public abstract class DatabaseSaveBuilder<T> {

    protected final List<String> uniqueKeyFields = new ArrayList<>();
    protected final List<Object> uniqueKeyValues = new ArrayList<>();

    protected final List<String> fields = new ArrayList<>();
    protected final List<Object> values = new ArrayList<>();

    protected final DatabaseTable table;
    protected final String idField;

    @Nullable protected final T idValue;

    protected DatabaseSaveBuilder(DatabaseTable table, String idField, @Nullable T id) {
        this.table = table;
        this.idField = idField;
        this.idValue = id;
    }

    /**
     * Specify a natural key for this table. If the <code>id</code> is null and there is a unique key
     * match, UPDATE is called with this row and the existing primary key is returned, instead of INSERT.
     * If more than one uniqueKey, fluent-jdbc assumes a composite unique constraint, that is <em>all</em>
     * fields must match
     */
    @CheckReturnValue
    public DatabaseSaveBuilder<T> uniqueKey(String fieldName, @Nullable Object fieldValue) {
        uniqueKeyFields.add(fieldName);
        uniqueKeyValues.add(fieldValue);
        return this;
    }

    /**
     * Specify a column name to be saved
     */
    @CheckReturnValue
    public DatabaseSaveBuilder<T> setField(String fieldName, @Nullable Object fieldValue) {
        fields.add(fieldName);
        values.add(fieldValue);
        return this;
    }

    /**
     * Executes the <code>UPDATE</code> or <code>INSERT</code> statement and returns a
     * {@link DatabaseSaveResult} which explains what operation was executed.
     *
     * <ul>
     *     <li>If id was set and {@link #tableWhereId(Object)} returns no object, inserts a new row.</li>
     *     <li>If id was set and {@link #tableWhereId(Object)} returns a row, updates this row if
     *     {@link #differingFields(DatabaseRow, Connection)} is non empty or treats the row as unchanged otherwise</li>
     *     <li>If id was null and {@link #tableWhereUniqueKey()} returns no object, inserts a new row,
     *     generating a new primary key</li>
     *     <li>If id was null and {@link #tableWhereUniqueKey()} returns a row, updates this row if
     *     {@link #differingFields(DatabaseRow, Connection)} is non empty or treats the row as unchanged otherwise</li>
     * </ul>
     */
    @Nonnull
    public DatabaseSaveResult<T> execute(@Nonnull Connection connection) throws SQLException {
        if (this.idValue != null) {
            Optional<List<String>> difference = tableWhereId(this.idValue).singleObject(connection, row -> differingFields(row, connection));
            if (!difference.isPresent()) {
                insert(connection);
                return DatabaseSaveResult.inserted(this.idValue);
            } else if (!difference.get().isEmpty()) {
                update(connection, this.idValue);
                return DatabaseSaveResult.updated(this.idValue, difference.get());
            } else {
                return DatabaseSaveResult.unchanged(this.idValue);
            }
        } else if (hasUniqueKey()) {
            AtomicReference<T> idValueLocal = new AtomicReference<>();
            Optional<List<String>> difference = tableWhereUniqueKey().singleObject(connection, row -> {
                idValueLocal.set(getId(row));
                return differingFields(row, connection);
            });
            if (!difference.isPresent()) {
                idValueLocal.set(insert(connection));
                return DatabaseSaveResult.inserted(idValueLocal.get());
            } else if (!difference.get().isEmpty()) {
                update(connection, idValueLocal.get());
                return DatabaseSaveResult.updated(idValueLocal.get(), difference.get());
            } else {
                return DatabaseSaveResult.unchanged(idValueLocal.get());
            }
        } else {
            return DatabaseSaveResult.inserted(insert(connection));
        }
    }

    /**
     * Creates a query where primary key is specified
     */
    @CheckReturnValue
    protected DatabaseTableQueryBuilder tableWhereId(T p) {
        return table.where(idField, p);
    }

    /**
     * Creates a query where all unique fields are specified
     */
    @CheckReturnValue
    protected DatabaseTableQueryBuilder tableWhereUniqueKey() {
        return table.whereAll(uniqueKeyFields, uniqueKeyValues);
    }

    /**
     * Compares the values on the {@link DatabaseRow} with the fields specified in this object and
     * returns the columns in the database with a different value
     */
    @CheckReturnValue
    protected List<String> differingFields(DatabaseRow row, Connection connection) throws SQLException {
        List<String> difference = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (!DatabaseStatement.dbValuesAreEqual(values.get(i), row, field, connection)) {
                difference.add(field);
            }
        }
        for (int i = 0; i < uniqueKeyFields.size(); i++) {
            String field = uniqueKeyFields.get(i);
            if (!DatabaseStatement.dbValuesAreEqual(uniqueKeyValues.get(i), row, field, connection)) {
                difference.add(field);
            }
        }
        return difference;
    }

    /**
     * Returns true if at least one field was specified as {@link #uniqueKey(String, Object)}
     * and all uniqueKeyValues are non-null
     */
    @CheckReturnValue
    protected boolean hasUniqueKey() {
        if (uniqueKeyFields.isEmpty()) return false;
        for (Object o : uniqueKeyValues) {
            if (o == null) return false;
        }
        return true;
    }

    /**
     * Build and execute the <code>INSERT</code>-statement to insert this row
     */
    @Nullable
    protected abstract T insert(Connection connection) throws SQLException;

    protected T insertWithId(T idValue, Connection connection) {
        table.insert()
                .setFields(fields, values)
                .setField(idField, idValue)
                .setFields(uniqueKeyFields, uniqueKeyValues)
                .execute(connection);
        return idValue;
    }


    /**
     * Build and execute the <code>UPDATE</code>-statement to update this row
     */
    protected T update(Connection connection, T idValue) {
        tableWhereId(idValue)
                .update()
                .setFields(this.fields, this.values)
                .setFields(uniqueKeyFields, uniqueKeyValues)
                .execute(connection);
        return idValue;
    }

    @SuppressWarnings("unchecked")
    @CheckReturnValue
    protected T getId(DatabaseRow row) throws SQLException {
        return (T) row.getObject(idField);
    }

}
