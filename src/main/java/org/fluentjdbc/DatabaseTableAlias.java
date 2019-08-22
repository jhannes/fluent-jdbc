package org.fluentjdbc;

import java.util.Objects;

public class DatabaseTableAlias {
    private final DatabaseTable table;
    private final String alias;

    public DatabaseTableAlias(DatabaseTable table, String alias) {
        this.table = table;
        this.alias = alias;
    }

    public DatabaseJoinedQueryBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        return new DatabaseJoinedQueryBuilder(this).join(a, b);
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
    public int hashCode() {
        return Objects.hash(table.getTableName().toUpperCase(), alias.toUpperCase());
    }

    public String getTableNameAndAlias() {
        return getTableName() + " " + getAlias();
    }

}
