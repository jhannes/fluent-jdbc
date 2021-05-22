package org.fluentjdbc;

import javax.annotation.CheckReturnValue;

/**
 * Used to specify a table in a {@link DatabaseJoinedQueryBuilder} joined <code>SELECT</code>
 * statement. The same table can be joined several times by using different aliases. Example:
 *
 * <pre>
 * DatabaseTableAlias perm = permissions.alias("perm");
 * DatabaseTableAlias m = memberships.alias("m");
 * DatabaseTableAlias p = persons.alias("p");
 * DatabaseTableAlias g = persons.alias("granter");
 *
 * perm.join(perm.column("membership_id"), m.column("id"))
 *         .join(m.column("person_id"), p.column("id"))
 *         .join(perm.column("granted_by"), g.column("id"))
 *         .list(connection, ....);
 * </pre>
 */
@CheckReturnValue
public class DatabaseTableAlias {
    private final String alias;
    private final String tableName;
    private final DatabaseTableReporter reporter;

    public DatabaseTableAlias(String tableName, String alias, DatabaseTableReporter reporter) {
        this.alias = alias;
        this.tableName = tableName;
        this.reporter = reporter;
    }

    public DatabaseTableAlias(String tableName, String alias) {
        this(tableName, alias, DatabaseTableReporter.LOGGING_REPORTER);
    }

    /**
     * Create a new {@link DatabaseJoinedQueryBuilder} based on this table by joining the
     * specified {@link DatabaseColumnReference}
     *
     * @see DatabaseJoinedQueryBuilder#join(DatabaseColumnReference, DatabaseColumnReference)
     */
    public DatabaseJoinedQueryBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        return select().join(a, b);
    }

    /**
     * Create a new {@link DatabaseJoinedQueryBuilder} based on this table by left joining the
     * specified {@link DatabaseColumnReference}.
     *
     * @see DatabaseJoinedQueryBuilder#leftJoin(DatabaseColumnReference, DatabaseColumnReference)
     */
    public DatabaseJoinedQueryBuilder leftJoin(DatabaseColumnReference a, DatabaseColumnReference b) {
        return select().leftJoin(a, b);
    }

    /**
     * Create a new {@link DatabaseJoinedQueryBuilder} with the specified query
     *
     * <p>NOTE: Maybe {@link DatabaseTableAlias} ought to implement all of {@link DatabaseQueryable}</p>
     */
    public DatabaseJoinedQueryBuilder where(String fieldName, String value) {
        return select().where(fieldName, value);
    }

    /**
     * Create a new {@link DatabaseJoinedQueryBuilder}
     */
    public DatabaseJoinedQueryBuilder select() {
        return new DatabaseJoinedQueryBuilder(this, reporter.operation("SELECT"));
    }

    public String getTableName() {
        return tableName;
    }

    public String getAlias() {
        return alias;
    }

    /**
     * Create a new {@link DatabaseColumnReference} object using this table, alias and the specified
     * column name
     */
    public DatabaseColumnReference column(String columnName) {
        return new DatabaseColumnReference(this, columnName);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + tableName + " " + getAlias() + "]";
    }

    /**
     * Returns the text to be used in the <code>JOIN tablename alias</code>
     * expression
     */
    public String getTableNameAndAlias() {
        return getTableName() + " " + getAlias();
    }

}
