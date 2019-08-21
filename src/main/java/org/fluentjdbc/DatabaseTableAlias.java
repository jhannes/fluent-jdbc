package org.fluentjdbc;

import java.util.Objects;

public class DatabaseTableAlias {
    private final DatabaseTable table;
    private final String alias;

    public DatabaseTableAlias(DatabaseTable table, String alias) {
        this.table = table;
        this.alias = alias;
    }

    public DatabaseJoinedQueryBuilder where(String fieldName, String value) {
        return new DatabaseJoinedQueryBuilder(this).where(fieldName, value);
    }

    public String getTableName() {
        return table.getTableName();
    }

    public String getAlias() {
        return alias;
    }

    public DatabaseColumnReference column(String columnName) {
        return new DatabaseColumnReference(this, columnName);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + table.getTableName() + " " + getAlias() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseTableAlias that = (DatabaseTableAlias) o;
        return getTableName().equalsIgnoreCase(that.getTableName()) &&
                alias.equalsIgnoreCase(that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table.getTableName().toUpperCase(), alias.toUpperCase());
    }

    public String getTableNameAndAlias() {
        return getTableName() + " " + getAlias();
    }
}
