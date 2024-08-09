package org.fluentjdbc;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
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

    protected DatabaseWhereBuilder whereClause = new DatabaseWhereBuilder();
    protected final DatabaseTable table;
    protected final List<String> orderByClauses = new ArrayList<>();
    protected Integer offset;
    protected Integer rowCount;

    DatabaseTableQueryBuilder(DatabaseTable table) {
        this.table = table;
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @Override
    public int getCount(Connection connection) {
        String statement = "select count(*) as count " + fromClause() + whereClause.whereClause();
        return table.newStatement("COUNT", statement, whereClause.getParameters())
                .singleObject(connection, row -> row.getInt("count"))
                .orElseThrow();
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(connection, row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> Stream<T> stream(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return createSelect().stream(connection, mapper);
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    @Override
    public void forEach(Connection connection, DatabaseResult.RowConsumer consumer) {
        createSelect().forEach(connection, consumer);
    }

    public DatabaseStatement createSelect() {
        return table.newStatement("SELECT", createSelectStatement(), whereClause.getParameters());
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
        return createSelect().singleObject(connection, mapper);
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpressionWithParameterList("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    public DatabaseTableQueryBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        this.whereClause = whereClause.whereExpressionWithParameterList(expression, parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereColumnValues("json_column", "?::json", jsonString)</code>
     */
    public DatabaseTableQueryBuilder whereColumnValuesEqual(String columnValue, String columnExpression, Collection<?> parameters) {
        this.whereClause = whereClause.whereColumnValuesEqual(columnValue, columnExpression, parameters);
        return this;
    }

    /**
     * Creates a {@link DatabaseUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    @Override
    public DatabaseUpdateBuilder update() {
        return table.update().where(whereClause);
    }

    /**
     * Creates a {@link DatabaseInsertOrUpdateBuilder} object to fluently generate a statement that will result
     * in either an <code>UPDATE ...</code> or <code>INSERT ...</code> depending on whether the row exists already
     */
    @Override
    public DatabaseInsertOrUpdateBuilder insertOrUpdate() {
        return table.insertOrUpdate().where(whereClause);
    }

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code>
     */
    @Override
    public int delete(Connection connection) {
        return table.delete().where(whereClause).execute(connection);
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
    @Override
    public DatabaseTableQueryBuilder skipAndLimit(int offset, int rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
        return this;
    }

    /**
     * Implemented as <code>return this</code> for compatibility purposes
     */
    @Override
    public DatabaseTableQueryBuilder query() {
        return this;
    }

    private String createSelectStatement() {
        return "select *" + fromClause() + whereClause.whereClause() + orderByClause() + fetchClause();
    }

    protected String fromClause() {
        return " from " + table.getTableName();
    }

    protected String orderByClause() {
        return orderByClauses.isEmpty() ? "" : " order by " + String.join(", ", orderByClauses);
    }

    private String fetchClause() {
        return rowCount == null ? "" : " offset " + offset + " rows fetch first " + rowCount + " rows only";
    }

}
