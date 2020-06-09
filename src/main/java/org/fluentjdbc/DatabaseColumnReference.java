package org.fluentjdbc;

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

    public String getColumnName() {
        return columnName;
    }
}
