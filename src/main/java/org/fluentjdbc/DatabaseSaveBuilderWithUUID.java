package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DatabaseSaveBuilderWithUUID extends DatabaseStatement {

    private List<String> uniqueKeyFields = new ArrayList<>();
    private List<Object> uniqueKeyValues = new ArrayList<>();

    private List<String> fields = new ArrayList<>();
    private List<Object> values = new ArrayList<>();
    private UUID idValue;
    private DatabaseTableImpl table;
    private String idField;

    public DatabaseSaveBuilderWithUUID(DatabaseTableImpl table, String idField, UUID idValue) {
        this.table = table;
        this.idField = idField;
        this.idValue = idValue;
    }

    @Nonnull
    public UUID execute(Connection connection) {
        if (idValue != null) {
            Boolean isSame = table.where(idField, this.idValue).singleObject(connection, new RowMapper<Boolean>() {
                @Override
                public Boolean mapRow(DatabaseRow row) throws SQLException {
                    return valuesAreUnchanged(row);
                }
            });
            if (isSame != null && !isSame) {
                update(connection, idValue);
            } else if (isSame == null) {
                insert(connection);
            }
            return idValue;
        } else if (hasUniqueKey()) {
            Boolean isSame = table.whereAll(uniqueKeyFields, uniqueKeyValues).singleObject(connection, new RowMapper<Boolean>() {
                @Override
                public Boolean mapRow(DatabaseRow row) throws SQLException {
                    idValue = UUID.fromString(row.getString(idField));
                    return valuesAreUnchanged(row);
                }
            });
            if (idValue != null && isSame != null && !isSame) {
                update(connection, idValue);
            } else if (idValue == null) {
                insert(connection);
            }
            return idValue;
        } else {
            return insert(connection);
        }
    }

    private UUID update(Connection connection, UUID idValue) {
        table.where("id", idValue).update()
            .setFields(uniqueKeyFields, uniqueKeyValues)
            .setFields(fields, values)
            .execute(connection);
        return idValue;
    }

    private boolean hasUniqueKey() {
        if (uniqueKeyFields.isEmpty()) return false;
        for (Object o : uniqueKeyValues) {
            if (o == null) return false;
        }
        return true;
    }

    private UUID insert(Connection connection) {
        if (idValue == null) {
            idValue = UUID.randomUUID();
        }
        table.insert()
                .setFields(fields, values)
                .setField(idField, idValue)
                .setFields(uniqueKeyFields, uniqueKeyValues)
                .execute(connection);
        return idValue;
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

    public DatabaseSaveBuilderWithUUID uniqueKey(String fieldName, @Nullable Object fieldValue) {
        uniqueKeyFields.add(fieldName);
        uniqueKeyValues.add(fieldValue);
        return this;
    }

    public DatabaseSaveBuilderWithUUID setField(String fieldName, @Nullable Object fieldValue) {
        fields.add(fieldName);
        values.add(fieldValue);
        return this;
    }
}
