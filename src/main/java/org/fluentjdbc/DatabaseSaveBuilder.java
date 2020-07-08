package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public abstract class DatabaseSaveBuilder<T> extends DatabaseStatement {

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

    public DatabaseSaveBuilder<T> uniqueKey(String fieldName, @Nullable Object fieldValue) {
        uniqueKeyFields.add(fieldName);
        uniqueKeyValues.add(fieldValue);
        return this;
    }

    public DatabaseSaveBuilder<T> setField(String fieldName, @Nullable Object fieldValue) {
        fields.add(fieldName);
        values.add(fieldValue);
        return this;
    }

    @Nonnull
    public DatabaseSaveResult<T> execute(Connection connection) throws SQLException {
        AtomicReference<T> idValueLocal = new AtomicReference<>(this.idValue);
        if (idValueLocal.get() != null) {
            Optional<List<String>> difference = tableWhereId(idValueLocal.get()).singleObject(connection, row -> differingFields(row, connection));
            if (!difference.isPresent()) {
                insert(connection);
                return DatabaseSaveResult.inserted(idValueLocal.get());
            } else if (!difference.get().isEmpty()) {
                update(connection, idValueLocal.get());
                return DatabaseSaveResult.updated(idValueLocal.get(), difference.get());
            } else {
                return DatabaseSaveResult.unchanged(idValueLocal.get());
            }
        } else if (hasUniqueKey()) {
            Optional<List<String>> difference = table.whereAll(uniqueKeyFields, uniqueKeyValues).singleObject(connection, row -> {
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

    protected DatabaseSimpleQueryBuilder tableWhereId(T p) {
        return table.where(idField, p);
    }

    private List<String> differingFields(DatabaseRow row, Connection connection) throws SQLException {
        List<String> difference = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (!dbValuesAreEqual(values.get(i), row, field, connection)) {
                difference.add(field);
            }
        }
        for (int i = 0; i < uniqueKeyFields.size(); i++) {
            String field = uniqueKeyFields.get(i);
            if (!dbValuesAreEqual(uniqueKeyValues.get(i), row, field, connection)) {
                difference.add(field);
            }
        }
        return difference;
    }

    private boolean hasUniqueKey() {
        if (uniqueKeyFields.isEmpty()) return false;
        for (Object o : uniqueKeyValues) {
            if (o == null) return false;
        }
        return true;
    }

    @Nullable
    protected abstract T insert(Connection connection) throws SQLException;

    protected T update(Connection connection, T idValue) {
        tableWhereId(idValue)
                .update()
                .setFields(this.fields, this.values)
                .setFields(uniqueKeyFields, uniqueKeyValues)
                .execute(connection);
        return idValue;
    }

    @SuppressWarnings("unchecked")
    protected T getId(DatabaseRow row) throws SQLException {
        return (T) row.getObject(idField);
    }

}
