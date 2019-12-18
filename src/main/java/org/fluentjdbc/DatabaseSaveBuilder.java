package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public abstract class DatabaseSaveBuilder<T> extends DatabaseStatement {

    protected List<String> uniqueKeyFields = new ArrayList<>();
    protected List<Object> uniqueKeyValues = new ArrayList<>();

    protected List<String> fields = new ArrayList<>();
    protected List<Object> values = new ArrayList<>();

    protected final DatabaseTable table;
    protected String idField;

    @Nullable protected T idValue;

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
        T idValue = this.idValue;
        if (idValue != null) {
            Boolean isSame = tableWhereId(this.idValue).singleObject(connection, this::shouldSkipRow);
            if (isSame != null && !isSame) {
                update(connection, idValue);
                return DatabaseSaveResult.updated(idValue);
            } else if (isSame == null) {
                insert(connection);
                return DatabaseSaveResult.inserted(idValue);
            } else {
                return DatabaseSaveResult.unchanged(idValue);
            }
        } else if (hasUniqueKey()) {
            Boolean isSame = table.whereAll(uniqueKeyFields, uniqueKeyValues).singleObject(connection, row -> {
                DatabaseSaveBuilder.this.idValue = getId(row);
                return shouldSkipRow(row);
            });
            idValue = this.idValue;
            if (idValue == null) {
                idValue = insert(connection);
                return DatabaseSaveResult.inserted(idValue);
            } else if (isSame != null && !isSame) {
                update(connection, idValue);
                return DatabaseSaveResult.updated(idValue);
            } else {
                return DatabaseSaveResult.unchanged(idValue);
            }
        } else {
            idValue = insert(connection);
            return DatabaseSaveResult.inserted(idValue);
        }
    }

    protected DatabaseSimpleQueryBuilder tableWhereId(T p) {
        return table.where(idField, p);
    }

    private boolean shouldSkipRow(DatabaseRow row) throws SQLException {
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (!Objects.equals(values.get(i), row.getObject(field))) return false;
        }
        for (int i = 0; i < uniqueKeyFields.size(); i++) {
            String field = uniqueKeyFields.get(i);
            if (!Objects.equals(uniqueKeyValues.get(i), row.getObject(field))) return false;
        }
        return true;
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
