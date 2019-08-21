package org.fluentjdbc;

import java.util.Objects;

public class DatabaseColumnReference {
    private final DatabaseTableAlias alias;
    private final String columnName;

    DatabaseColumnReference(DatabaseTableAlias alias, String columnName) {
        this.alias = alias;
        this.columnName = columnName;
    }

    public String getTableName() {
        return alias.getTableName();
    }

    public String getQualifiedColumnName() {
        return alias.getAlias() + "." + columnName;
    }

    public DatabaseTableAlias getTableAlias() {
        return alias;
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
        return getClass().getSimpleName() + "[" + getQualifiedColumnName() + " in " + alias.getTableNameAndAlias() + "]";
    }
}
