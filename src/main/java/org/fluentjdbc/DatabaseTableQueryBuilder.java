package org.fluentjdbc;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Generate <code>SELECT</code> statements by collecting <code>WHERE</code> expressions and parameters.Example:
 *
 * <pre>
 *  List&lt;Person&gt; result = table
 *     .where("firstName", firstName)
 *     .whereExpression("lastName like ?", "joh%")
 *     .whereIn("status", statuses)
 *     .orderBy("lastName")
 *     .list(connection, row -&gt; new Person(row));
 * </pre>
 */
@ParametersAreNonnullByDefault
public class DatabaseTableQueryBuilder implements
        DatabaseSimpleQueryBuilder<DatabaseTableQueryBuilder>,
        DatabaseListableQueryBuilder<DatabaseTableQueryBuilder>
{

    protected final DatabaseTable table;
    protected final DatabaseWhereBuilder whereBuilder = new DatabaseWhereBuilder();
    protected final List<String> orderByClauses = new ArrayList<>();

    DatabaseTableQueryBuilder(DatabaseTable table) {
        this.table = table;
    }

    /**
     * Add the arguments to the column list for the <code>SELECT column, column...</code> statement
     */
    public DatabaseSelectBuilder select(String... columns) {
        return createSelectBuilder().select(columns);
    }

    /**
     * Adds the parameter to the WHERE-clause and all the parameter list.
     * E.g. <code>where(new DatabaseQueryParameter("created_at between ? and ?", List.of(earliestDate, latestDate)))</code>
     */
    @Override
    public DatabaseTableQueryBuilder where(DatabaseQueryParameter parameter) {
        whereBuilder.where(parameter);
        return this;
    }

    /**
     * Creates a {@link DatabaseUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    @Override
    public DatabaseUpdateBuilder update() {
        return table.update().where(whereBuilder);
    }

    /**
     * Creates a {@link DatabaseInsertOrUpdateBuilder} object to fluently generate a statement that will result
     * in either an <code>UPDATE ...</code> or <code>INSERT ...</code> depending on whether the row exists already
     */
    @Override
    public DatabaseInsertOrUpdateBuilder insertOrUpdate() {
        return table.insertOrUpdate().where(whereBuilder);
    }

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code>
     */
    @Override
    public int delete(Connection connection) {
        return table.delete().where(whereBuilder).execute(connection);
    }

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @Override
    public DatabaseTableQueryBuilder orderBy(String orderByClause) {
        orderByClauses.add(orderByClause);
        return this;
    }

    /**
     * Sets the <code>ORDER BY ...</code> clause of the <code>SELECT</code> statement
     */
    @Override
    public DatabaseTableQueryBuilder orderBy(List<String> orderByClauses) {
        this.orderByClauses.clear();
        this.orderByClauses.addAll(orderByClauses);
        return this;
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link #list(Connection, DatabaseResult.RowMapper)}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @Override
    public DatabaseTableQueryBuilder unordered() {
        return this;
    }

    /**
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    public DatabaseSelectBuilder skipAndLimit(Integer offset, Integer rowCount) {
        return createSelectBuilder().skipAndLimit(offset, rowCount);
    }

    /**
     * If the query returns no rows, returns {@link SingleRow#absent}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param connection Database connection
     * @param mapper Function object to map a single returned row to an object
     * @return the mapped row if one row is returned, {@link SingleRow#absent} otherwise
     * @throws MultipleRowsReturnedException if more than one row was matched the query
     */
    @Nonnull
    @Override
    public <T> SingleRow<T> singleObject(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return createSelectBuilder().singleObject(connection, mapper);
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(connection, row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> Stream<T> stream(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return createSelectBuilder().stream(connection, mapper);
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    @Override
    public void forEach(Connection connection, DatabaseResult.RowConsumer consumer) {
        createSelectBuilder().forEach(connection, consumer);
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @Override
    public int getCount(Connection connection) {
        return createSelectBuilder().getCount(connection);
    }

    /**
     * Implemented as <code>return this</code> for compatibility purposes
     */
    @Override
    public DatabaseTableQueryBuilder query() {
        return this;
    }

    public DatabaseSelectBuilder createSelectBuilder() {
        return new DatabaseSelectBuilder(table.getFactory(), whereBuilder)
                .orderBy(orderByClauses)
                .from(table.getTableName());
    }
}
