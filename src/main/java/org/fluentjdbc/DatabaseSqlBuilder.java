package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Used to construct SQL SELECT statements in a flexible way with {@link #where(String, Object)}
 * clauses, {@link #select(String...)} column names, {@link #from(String)} table statement,
 * {@link #groupBy(String...)}, {@link #orderBy(String)} and {@link #skipAndLimit(int, int)}.
 *
 * Example:
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
public class DatabaseSqlBuilder implements DatabaseQueryBuilder<DatabaseSqlBuilder>, DatabaseListableQueryBuilder<DatabaseSqlBuilder> {

    private final ArrayList<String> columns = new ArrayList<>();
    private final DatabaseStatementFactory factory;
    private String fromStatement;
    private final ArrayList<String> groupByClauses = new ArrayList<>();
    private final ArrayList<String> orderByClauses = new ArrayList<>();
    private Integer offset;
    private Integer rowCount;
    private final DatabaseWhereBuilder whereBuilder = new DatabaseWhereBuilder();

    public DatabaseSqlBuilder(DatabaseStatementFactory factory) {
        this.factory = factory;
    }

    /**
     * Add the arguments to the column list for the <code>SELECT column, column...</code> statement
     */
    public DatabaseSqlBuilder select(String... columns) {
        this.columns.addAll(Arrays.asList(columns));
        return this;
    }

    /**
     * Replace the from part of the <code>SELECT ... FROM fromStatement</code> in the select statement
     */
    public DatabaseSqlBuilder from(String fromStatement) {
        this.fromStatement = fromStatement;
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpressionWithParameterList("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    @Override
    public DatabaseSqlBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        //noinspection ResultOfMethodCallIgnored
        whereBuilder.whereExpressionWithParameterList(expression, parameters);
        return this;
    }

    /**
     * Add the arguments to the column list for the <code>SELECT column, column...</code> statement
     */
    public DatabaseSqlBuilder groupBy(String... groupByStatement) {
        groupByClauses.addAll(Arrays.asList(groupByStatement));
        return this;
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @Override
    public DatabaseSqlBuilder unordered() {
        return this;
    }

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @Override
    public DatabaseSqlBuilder orderBy(String orderByClause) {
        orderByClauses.add(orderByClause);
        return this;
    }

    /**
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @Override
    public DatabaseSqlBuilder skipAndLimit(int offset, int rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
        return this;
    }

    /**
     * If the query returns no rows, returns {@link Optional#empty()}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param mapper Function object to map a single returned row to a object
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @Override
    public <OBJECT> Optional<OBJECT> singleObject(Connection connection, DatabaseResult.RowMapper<OBJECT> mapper) {
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
        return factory.newStatement("*", "COUNT", selectStatement, whereBuilder.getParameters())
                .singleObject(connection, row -> row.getInt("count"))
                .orElseThrow(() -> new RuntimeException("Should never happen"));
    }

    /**
     * Implemented as <code>return this</code> for compatibility purposes
     */
    @Override
    public DatabaseSqlBuilder query() {
        return this;
    }

    @Nonnull
    private DatabaseStatement getDatabaseStatement() {
        return factory.newStatement("*", "SELECT", createSelectStatement(String.join(", ", columns)), whereBuilder.getParameters());
    }

    private String createSelectStatement(String columns) {
        return "select " + columns
                + (" from " + fromStatement)
                + whereBuilder.whereClause()
                + (groupByClauses.isEmpty() ? "" : " group by " + String.join(", ", groupByClauses))
                + (orderByClauses.isEmpty() ? "" : " order by " + String.join(", ", orderByClauses))
                + (rowCount == null ? "" : " offset " + offset + " rows fetch first " + rowCount + " rows only");
    }
}
