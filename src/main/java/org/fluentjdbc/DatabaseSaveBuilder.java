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

    private List<String> columns = new ArrayList<>();
    private List<Object> parameters = new ArrayList<>();

    private final DatabaseTable table;
    private String idField;
    private Number id;

    DatabaseSaveBuilder(DatabaseTable table, String idField, @Nullable Number id) {
        this.table = table;
        this.idField = idField;
        this.id = id;
    }

    public DatabaseSaveBuilder uniqueKey(String fieldName, @Nullable Object fieldValue) {
        uniqueKeyFields.add(fieldName);
        uniqueKeyValues.add(fieldValue);
        columns.add(fieldName);
        parameters.add(fieldValue);
        return this;
    }

    public DatabaseSaveBuilder setField(String fieldName, @Nullable Object fieldValue) {
        columns.add(fieldName);
        parameters.add(fieldValue);
        return this;
    }

    public long execute(Connection connection) {
        if (id == null && hasUniqueKey()) {
            id = table.whereAll(uniqueKeyFields, uniqueKeyValues).singleLong(connection, idField);
        }

        if (id != null) {
            Number id = table.where(idField, this.id).singleLong(connection, idField);
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
            .setField(idField, id)
            .setFields(columns, parameters)
            .execute(connection);
        return id.longValue();
    }

    private long update(Connection connection) {
        table.where("id", id).update(connection, this.columns, this.parameters);
        return id.longValue();
    }

    private long insert(Connection connection) {
        return table.insert()
                .setFields(columns, parameters)
                .generateKeyAndInsert(connection);
    }
}
