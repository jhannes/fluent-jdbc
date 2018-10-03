package org.fluentjdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseInsertBuilder extends DatabaseStatement {

    private List<String> fieldNames = new ArrayList<>();
    private List<Object> parameters = new ArrayList<>();
    private DatabaseTable table;

    public DatabaseInsertBuilder(DatabaseTable table) {
        this.table = table;
    }

    List<Object> getParameters() {
        return parameters;
    }

    public DatabaseInsertBuilder setField(String fieldName, @Nullable Object parameter) {
        this.fieldNames.add(fieldName);
        this.parameters.add(parameter);
        return this;
    }

    public DatabaseInsertBuilder setFields(List<String> fieldNames, List<Object> parameters) {
        this.fieldNames.addAll(fieldNames);
        this.parameters.addAll(parameters);
        return this;
    }

    public void execute(Connection connection) {
        executeUpdate(createInsertStatement(), parameters, connection);
    }

    String createInsertStatement() {
        return createInsertSql(table.getTableName(), fieldNames);
    }

    // TODO: This doesn't work for Android when idValue is null
    public <T> DatabaseInsertWithPkBuilder<T> setPrimaryKey(String idField, @Nullable T idValue) {
        if (idValue != null) {
            setField(idField, idValue);
        }
        return new DatabaseInsertWithPkBuilder<>(this, idValue);
    }

}
