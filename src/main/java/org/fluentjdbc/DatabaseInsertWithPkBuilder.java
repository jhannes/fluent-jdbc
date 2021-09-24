package org.fluentjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.fluentjdbc.DatabaseStatement.bindParameters;

/**
 * Generate <code>INSERT</code> statements with automatic primary key generation by collecting
 * field names and parameters. Example:
 *
 * <pre>
 * Long id = table.insert()
 *    .setPrimaryKey("id", (Long)null) // this creates {@link DatabaseInsertWithPkBuilder}
 *    .setField("name", "Something")
 *    .setField("code", 102)
 *    .execute(connection);
 * </pre>
 */
public class DatabaseInsertWithPkBuilder<T> {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseInsertWithPkBuilder.class);

    private final DatabaseInsertBuilder insertBuilder;
    private final String idField;
    private final T idValue;

    public DatabaseInsertWithPkBuilder(DatabaseInsertBuilder insertBuilder, String idField, T idValue) {
        this.insertBuilder = insertBuilder;
        this.idField = idField;
        this.idValue = idValue;
    }

    /**
     * Adds fieldName to the <code>INSERT (fieldName) VALUES (?)</code> and parameter to the list of parameters
     */
    @CheckReturnValue
    public DatabaseInsertWithPkBuilder<T> setField(String fieldName, Object parameter) {
        //noinspection ResultOfMethodCallIgnored
        insertBuilder.setField(fieldName, parameter);
        return this;
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @CheckReturnValue
    public DatabaseInsertWithPkBuilder<T> setFields(List<String> fields, List<Object> values) {
        //noinspection ResultOfMethodCallIgnored
        insertBuilder.setFields(fields, values);
        return this;
    }

    /**
     * Executes the insert statement and returns the number of rows inserted. If a pre-specified
     * primary key was supplied, this is used, otherwise, uses the {@link PreparedStatement#getGeneratedKeys()}
     * to retrieve the generated keys
     */
    @Nonnull
    public T execute(Connection connection) {
        T idValue = this.idValue;
        if (idValue == null) {
            return insertBuilder.getTable()
                    .newStatement("INSERT", insertBuilder.createInsertStatement(), insertBuilder.getParameters())
                    .execute(connection, this::getGeneratedKey, new String[] { idField });
        } else {
            insertBuilder.execute(connection);
            return idValue;
        }
    }


    // TODO: This doesn't work for Android - we need to do select last_insert_rowid() explicitly (or update SQLDroid)
    @SuppressWarnings("unchecked")
    @CheckReturnValue
    private T getGeneratedKey(PreparedStatement stmt) throws SQLException {
        stmt.executeUpdate();
        try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
            generatedKeys.next();
            return (T) Long.valueOf(generatedKeys.getLong(1));
        }
    }
}
