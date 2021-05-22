package org.fluentjdbc;

import javax.annotation.CheckReturnValue;

/**
 * Describes a database column alias for a <code>SELECT * FROM table a JOIN table b ON a.column = b.column</code>
 * expression. {@link DatabaseColumnReference} binds together the table, table alias and column name
 */
@CheckReturnValue
public class DatabaseColumnReference {
    private final String columnName;
    private final DatabaseTableAlias alias;

    DatabaseColumnReference(DatabaseTableAlias alias, String columnName) {
        this.columnName = columnName;
        this.alias = alias;
    }

    public DatabaseTableAlias getTableAlias() {
        return alias;
    }

    /**
     * Returns the expression to be used for <code>JOIN ... ON alias.columnName = ...</code>
     */
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
