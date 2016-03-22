package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseSaveBuilder extends DatabaseStatement {

    private List<String> uniqueKeyFields = new ArrayList<>();
    private List<Object> uniqueKeyValues = new ArrayList<>();

    private List<String> fields = new ArrayList<>();
    private List<Object> values = new ArrayList<>();

    private final DatabaseTable table;
    private String idField;
    private Number idValue;

    DatabaseSaveBuilder(DatabaseTable table, String idField, @Nullable Number id) {
        this.table = table;
        this.idField = idField;
        this.idValue = id;
    }

    public DatabaseSaveBuilder uniqueKey(String fieldName, @Nullable Object fieldValue) {
        uniqueKeyFields.add(fieldName);
        uniqueKeyValues.add(fieldValue);
        return this;
    }

    public DatabaseSaveBuilder setField(String fieldName, @Nullable Object fieldValue) {
        fields.add(fieldName);
        values.add(fieldValue);
        return this;
    }

    public long execute(Connection connection) {
        if (idValue != null) {
            Boolean isSame = table.where(idField, this.idValue).singleObject(connection, row -> valuesAreUnchanged(row));
            if (isSame == null) {
                return insert(connection);
            } else if (!isSame) {
                return update(connection);
            } else {
                return idValue.longValue();
            }
        } else if (hasUniqueKey()) {
            Boolean isSame = table.whereAll(uniqueKeyFields, uniqueKeyValues).singleObject(connection, row -> {
                idValue = row.getLong(idField);
                return valuesAreUnchanged(row);
            });
            if (isSame == null) {
                return insert(connection);
            } else if (!isSame) {
                return update(connection);
            } else {
                return idValue.intValue();
            }
        } else {
            return insert(connection);
        }
    }

    private boolean valuesAreUnchanged(DatabaseRow row) throws SQLException {
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (!equal(values.get(i), row.getObject(field))) return false;
        }
        return true;
    }

    private boolean equal(Object o, Object db) {
        return Objects.equals(o, db);
    }

    private boolean hasUniqueKey() {
        if (uniqueKeyFields.isEmpty()) return false;
        for (Object o : uniqueKeyValues) {
            if (o == null) return false;
        }
        return true;
    }

    private long insert(Connection connection) {
        DatabaseInsertBuilder insertStatement = table.insert()
            .setPrimaryKey(idField, idValue)
            .setFields(fields, values)
            .setFields(uniqueKeyFields, uniqueKeyValues);
        return ((Number)insertStatement.execute(connection)).longValue();
    }

    private long update(Connection connection) {
        table.where("id", idValue)
                .update()
                .setFields(this.fields, this.values)
                .setFields(uniqueKeyFields, uniqueKeyValues)
                .execute(connection);
        return idValue.longValue();
    }

}
