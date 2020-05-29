package org.fluentjdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseInsertBuilder extends DatabaseStatement implements DatabaseUpdateable<DatabaseInsertBuilder> {

    private List<String> fieldNames = new ArrayList<>();
    private List<Object> parameters = new ArrayList<>();
    private String table;

    public DatabaseInsertBuilder(String table) {
        this.table = table;
    }

    List<Object> getParameters() {
        return parameters;
    }

    @Override
    public DatabaseInsertBuilder setField(String fieldName, @Nullable Object parameter) {
        this.fieldNames.add(fieldName);
        this.parameters.add(parameter);
        return this;
    }

    @Override
    public DatabaseInsertBuilder setFields(Collection<String> fieldNames, Collection<?> parameters) {
        this.fieldNames.addAll(fieldNames);
        this.parameters.addAll(parameters);
        return this;
    }

    public int execute(Connection connection) {
        return executeUpdate(createInsertStatement(), parameters, connection);
    }

    String createInsertStatement() {
        return createInsertSql(table, fieldNames);
    }

    // TODO: This doesn't work for Android when idValue is null
    public <T> DatabaseInsertWithPkBuilder<T> setPrimaryKey(String idField, @Nullable T idValue) {
        if (idValue != null) {
            setField(idField, idValue);
        }
        return new DatabaseInsertWithPkBuilder<>(this, idValue);
    }

}
