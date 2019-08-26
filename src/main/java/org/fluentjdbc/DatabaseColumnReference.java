package org.fluentjdbc;

import java.util.Objects;

public class DatabaseColumnReference {
    private final String columnName;
    private final String tableName;
    private final DatabaseTableAlias alias;

    DatabaseColumnReference(DatabaseTableAlias alias, String columnName) {
        this.columnName = columnName;
        this.tableName = alias.getTableName();
        this.alias = alias;
    }

    public DatabaseTableAlias getTableAlias() {
        return alias;
    }

    public String getTableName() {
        return tableName;
    }

    public String getQualifiedColumnName() {
        return alias.getAlias() + "." + columnName;
    }

    public String getTableNameAndAlias() {
        return alias.getTableNameAndAlias();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseColumnReference that = (DatabaseColumnReference) o;
        return Objects.equals(alias, that.alias) &&
                columnName.equalsIgnoreCase(that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, columnName.toUpperCase());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getQualifiedColumnName() + " in " + getTableNameAndAlias() + "]";
    }
}
