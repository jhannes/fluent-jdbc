package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Used to construct SQL SELECT statements in a flexible way with {@link #where(String, Object)}
 * clauses, {@link #select(String...)} column names, {@link #from(String)} table statement,
 * {@link #groupBy(String...)}, {@link #orderBy(String)} and {@link #skipAndLimit(int, int)}.
 *
 * <h2>Example:</h2>
 *
 * <pre>
 * new DatabaseSqlBuilder()
 *      .select("city, avg(age) as average_age")
 *      .from("person")
 *      .groupBy("city")
 *      .order("avg(age) desc")
 *      .limit(1)
 *      .singleString(connection, "city");
 * </pre>
 *
 */
public class DatabaseSelectBuilder implements DatabaseQueryBuilder<DatabaseSelectBuilder>, DatabaseListableQueryBuilder<DatabaseSelectBuilder> {

    private final DatabaseStatementFactory factory;
    private String fromStatement;
    private final ArrayList<String> groupByClauses = new ArrayList<>();

    private final ArrayList<String> columns = new ArrayList<>();
    protected final DatabaseWhereBuilder whereBuilder;
    protected final List<String> orderByClauses = new ArrayList<>();
    protected Integer offset;
    protected Integer rowCount;

    public DatabaseSelectBuilder(DatabaseStatementFactory factory) {
        this(factory, new DatabaseWhereBuilder());
    }

    public DatabaseSelectBuilder(DatabaseStatementFactory factory, DatabaseWhereBuilder whereBuilder) {
        this.factory = factory;
        this.whereBuilder = whereBuilder;
    }

    /**
     * Add the arguments to the column list for the <code>SELECT column, column...</code> statement
     */
    public DatabaseSelectBuilder select(String... columns) {
        this.columns.addAll(Arrays.asList(columns));
        return this;
    }

    /**
     * Replace the "from" part of the <code>SELECT ... FROM fromStatement</code> in the select statement
     */
    public DatabaseSelectBuilder from(String fromStatement) {
        this.fromStatement = fromStatement;
        return this;
    }

    /**
     * Adds the parameter to the WHERE-clause and all the parameter list.
     * E.g. <code>where(new DatabaseQueryParameter("created_at between ? and ?", List.of(earliestDate, latestDate)))</code>
     */
    @Override
    public DatabaseSelectBuilder where(DatabaseQueryParameter parameter) {
        whereBuilder.where(parameter);
        return this;
    }

    /**
     * Add the arguments to the column list for the <code>SELECT column, column...</code> statement
     */
    public DatabaseSelectBuilder groupBy(String... groupByStatement) {
        groupByClauses.addAll(Arrays.asList(groupByStatement));
        return this;
    }

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @Override
    public DatabaseSelectBuilder orderBy(String orderByClause) {
        orderByClauses.add(orderByClause);
        return this;
    }

    /**
     * Sets the <code>ORDER BY ...</code> clause of the <code>SELECT</code> statement
     */
    @Override
    public DatabaseSelectBuilder orderBy(List<String> orderByClauses) {
        this.orderByClauses.clear();
        this.orderByClauses.addAll(orderByClauses);
        return this;
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @Override
    public DatabaseSelectBuilder unordered() {
        return this;
    }

    /**
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    public DatabaseSelectBuilder skipAndLimit(int offset, int rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
        return this;
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
    public <OBJECT> SingleRow<OBJECT> singleObject(Connection connection, DatabaseResult.RowMapper<OBJECT> mapper) {
        return getDatabaseStatement().singleObject(connection, mapper);
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <OBJECT> Stream<OBJECT> stream(@Nonnull Connection connection, DatabaseResult.RowMapper<OBJECT> mapper) {
        return getDatabaseStatement().stream(connection, mapper);
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    @Override
    public void forEach(Connection connection, DatabaseResult.RowConsumer consumer) {
        getDatabaseStatement().forEach(connection, consumer);
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @Override
    public int getCount(Connection connection) {
        String selectStatement = "select count(*) as count "
                + (" from " + fromStatement)
                + whereBuilder.whereClause()
                + (groupByClauses.isEmpty() ? "" : " group by " + String.join(", ", groupByClauses));
        return factory.newStatement(fromStatement, "COUNT", selectStatement, whereBuilder.getParameters())
                .singleObject(connection, row -> row.getInt("count"))
                .orElseThrow();
    }

    /**
     * Implemented as <code>return this</code> for compatibility purposes
     */
    @Override
    public DatabaseSelectBuilder query() {
        return this;
    }

    /**
     * Returns a {@link DatabaseQueryParameter} with a <code>column in (SELECT ....)</code>
     * with this expression and the same parameters as this builder
     */
    public DatabaseQueryParameter asNestedSelectOn(String column) {
        return new DatabaseQueryParameter(column + " in (" + createSelectStatement() + ")", whereBuilder.getParameters());
    }

    @Nonnull
    protected DatabaseStatement getDatabaseStatement() {
        return factory.newStatement(fromStatement, "SELECT", createSelectStatement(), whereBuilder.getParameters());
    }

    protected String createSelectStatement() {
        String columns = this.columns.isEmpty() ? "*" : String.join(", ", this.columns);
        return "select " + columns
                + (" from " + fromStatement)
                + whereBuilder.whereClause()
                + (groupByClauses.isEmpty() ? "" : " group by " + String.join(", ", groupByClauses))
                + (orderByClauses.isEmpty() ? "" : " order by " + String.join(", ", orderByClauses))
                + (rowCount == null ? "" : " offset " + offset + " rows fetch first " + rowCount + " rows only");
    }
}
