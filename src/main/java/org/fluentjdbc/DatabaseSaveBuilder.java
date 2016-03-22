package org.fluentjdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

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
        if (idValue == null && hasUniqueKey()) {
            idValue = table.whereAll(uniqueKeyFields, uniqueKeyValues).singleLong(connection, idField);
        }

        if (idValue != null) {
            Number id = table.where(idField, this.idValue).singleLong(connection, idField);
            if (id != null) {
                return update(connection);
            } else {
                return insertWithId(connection);
            }
        } else {
            return insert(connection);
        }
    }

    private boolean hasUniqueKey() {
        if (uniqueKeyFields.isEmpty()) return false;
        for (Object o : uniqueKeyValues) {
            if (o == null) return false;
        }
        return true;
    }

    private long insertWithId(Connection connection) {
        table.insert()
            .setField(idField, idValue)
            .setFields(fields, values)
            .setFields(uniqueKeyFields, uniqueKeyValues)
            .execute(connection);
        return idValue.longValue();
    }

    private long insert(Connection connection) {
        return table.insert()
                .setFields(fields, values)
                .setFields(uniqueKeyFields, uniqueKeyValues)
                .generateKeyAndInsert(connection);
    }

    private long update(Connection connection) {
        table.where("id", idValue).update()
                .setFields(this.fields, this.values)
                .setFields(uniqueKeyFields, uniqueKeyValues)
                .execute(connection);
        return idValue.longValue();
    }

}
