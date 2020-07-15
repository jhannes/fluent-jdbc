package org.fluentjdbc;

import javax.annotation.Nullable;

/**
 * Used to specify a table in a {@link DbContextJoinedSelectBuilder} joined <code>SELECT</code>
 * statement. The same table can be joined several times by using different aliases. Example:
 *
 * <pre>
 * DbContextTableAlias perm = permissions.alias("perm");
 * DbContextTableAlias m = memberships.alias("m");
 * DbContextTableAlias p = persons.alias("p");
 * DbContextTableAlias g = persons.alias("granter");
 *
 * perm.join(perm.column("membership_id"), m.column("id"))
 *         .join(m.column("person_id"), p.column("id"))
 *         .join(perm.column("granted_by"), g.column("id"))
 *         .list(....);
 * </pre>
 */
public class DbContextTableAlias {
    private final DbTableContext dbTableContext;
    private final DatabaseTableAlias alias;

    public DbContextTableAlias(DbTableContext dbTableContext, String alias) {
        this.dbTableContext = dbTableContext;
        this.alias = dbTableContext.getTable().alias(alias);
    }

    /**
     * Create a new {@link DatabaseColumnReference} object using this table, alias and the specified
     * column name
     */
    public DatabaseColumnReference column(String columnName) {
        return alias.column(columnName);
    }

    /**
     * Create a new {@link DbContextJoinedSelectBuilder} based on this table by joining the
     * specified {@link DatabaseColumnReference}
     * 
     * @see DbContextJoinedSelectBuilder#join(DatabaseColumnReference, DatabaseColumnReference)
     */
    public DbContextJoinedSelectBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        return select().join(a, b);
    }

    /**
     * Create a new {@link DbContextJoinedSelectBuilder} based on this table by left joining the
     * specified {@link DatabaseColumnReference}.
     * 
     * @see DbContextJoinedSelectBuilder#leftJoin(DatabaseColumnReference, DatabaseColumnReference) 
     */
    public DbContextJoinedSelectBuilder leftJoin(DatabaseColumnReference a, DatabaseColumnReference b) {
        return select().leftJoin(a, b);
    }

    public DatabaseTableAlias getTableAlias() {
        return alias;
    }

    /**
     * Create a new {@link DbContextJoinedSelectBuilder} with the specified query
     *
     * <p>NOTE: Maybe {@link DbContextTableAlias} ought to implement all of {@link DatabaseQueryable}</p>
     */
    public DbContextJoinedSelectBuilder where(String fieldName, @Nullable Object value) {
        return select().whereExpression(alias.getAlias() + "." + fieldName + " = ?", value);
    }

    /**
     * Create a new {@link DbContextJoinedSelectBuilder}
     */
    public DbContextJoinedSelectBuilder select() {
        return new DbContextJoinedSelectBuilder(this);
    }

    public DbContext getDbContext() {
        return dbTableContext.getDbContext();
    }

}
