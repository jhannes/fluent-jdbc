package org.fluentjdbc;

import java.util.Objects;

public class DatabaseTableAlias {
    private final String alias;
    private String tableName;

    public DatabaseTableAlias(String tableName, String alias) {
        this.alias = alias;
        this.tableName = tableName;
    }

    public DatabaseJoinedQueryBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        return new DatabaseJoinedQueryBuilder(this).join(a, b);
    }

    public DatabaseJoinedQueryBuilder where(String fieldName, String value) {
        return new DatabaseJoinedQueryBuilder(this).where(fieldName, value);
    }

    public String getTableName() {
        return tableName;
    }

    public String getAlias() {
        return alias;
    }

    public DatabaseColumnReference column(String columnName) {
        return new DatabaseColumnReference(this, columnName);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + tableName + " " + getAlias() + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName.toUpperCase(), alias.toUpperCase());
    }

    public String getTableNameAndAlias() {
        return getTableName() + " " + getAlias();
    }

}
