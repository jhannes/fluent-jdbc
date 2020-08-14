package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.fluentjdbc.DatabaseStatement.bindParameters;
import static org.fluentjdbc.DatabaseStatement.parameterString;

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
public class DatabaseTableQueryBuilder implements DatabaseSimpleQueryBuilder, DatabaseListableQueryBuilder {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseTableQueryBuilder.class);

    private final DatabaseTable table;
    private final List<String> conditions = new ArrayList<>();
    private final List<Object> parameters = new ArrayList<>();
    private final List<String> orderByClauses = new ArrayList<>();
    private Integer offset;
    private Integer rowCount;

    DatabaseTableQueryBuilder(DatabaseTable table) {
        this.table = table;
    }

    /**
     * Executes <code>SELECT count(*) FROM ...</code> on the query and returns the result
     */
    @Override
    public int getCount(Connection connection) {
        long startTime = System.currentTimeMillis();
        String query = "select count(*) as count " + fromClause() + whereClause() + orderByClause();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = new DatabaseResult(stmt, stmt.executeQuery())) {
                return result.single(row -> row.getInt("count")).orElseThrow(() -> new RuntimeException("Should never happen"));
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"", (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(connection, row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> Stream<T> stream(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        try {
            String query = createSelectStatement();
            logger.trace(query);
            PreparedStatement stmt = connection.prepareStatement(query);
            bindParameters(stmt, parameters);
            DatabaseResult result = new DatabaseResult(stmt, stmt.executeQuery());
            return result.stream(mapper, query);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(connection, row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> List<T> list(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return stream(connection, mapper).collect(Collectors.toList());
    }

    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    @Override
    public void forEach(Connection connection, DatabaseResult.RowConsumer consumer) {
        query(connection, result -> {
            result.forEach(consumer);
            return null;
        });
    }

    /**
     * If the query returns no rows, returns {@link Optional#empty()}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param connection Database connection
     * @param mapper Function object to map a single returned row to a object
     * @return the mapped row if one row is returned, Optional.empty otherwise
     * @throws IllegalStateException if more than one row was matched the the query
     */
    @Nonnull
    @Override
    public <T> Optional<T> singleObject(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return query(connection, result -> result.single(mapper));
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @Override
    public DatabaseSimpleQueryBuilder whereOptional(String fieldName, @Nullable Object value) {
        if (value == null) return this;
        return where(fieldName, value);
    }

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    public DatabaseSimpleQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        if (parameters.isEmpty()) {
            return whereExpression(fieldName + " <> " + fieldName);
        }
        whereExpression(fieldName + " IN (" + parameterString(parameters.size()) + ")");
        this.parameters.addAll(parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    public DatabaseSimpleQueryBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters) {
        whereExpression(expression);
        this.parameters.addAll(parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and the value to the parameter list. E.g.
     * <code>whereExpression("created_at &gt; ?", earliestDate)</code>
     */
    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression, @Nullable Object parameter) {
        whereExpression(expression);
        parameters.add(parameter);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause
     */
    @Override
    public DatabaseSimpleQueryBuilder whereExpression(String expression) {
        conditions.add(expression);
        return this;
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    public DatabaseSimpleQueryBuilder whereAll(List<String> fields, List<Object> values) {
        for (int i = 0; i < fields.size(); i++) {
            where(fields.get(i), values.get(i));
        }
        return this;
    }

    /**
     * Creates a {@link DatabaseUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     */
    @Override
    public DatabaseUpdateBuilder update() {
        return table.update().setWhereFields(conditions, parameters);
    }

    /**
     * Executes <code>DELETE FROM tableName WHERE ....</code>
     */
    @Override
    public int delete(Connection connection) {
        return table.delete().setWhereFields(conditions, parameters).execute(connection);
    }

    /**
     * Adds <code>ORDER BY ...</code> clause to the <code>SELECT</code> statement
     */
    @Override
    public DatabaseListableQueryBuilder orderBy(String orderByClause) {
        orderByClauses.add(orderByClause);
        return this;
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link #list(Connection, DatabaseResult.RowMapper)}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @Override
    public DatabaseListableQueryBuilder unordered() {
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
     * Returns this. Needed to make {@link DatabaseTableQueryBuilder} interchangeable with {@link DatabaseTable}
     */
    @Override
    public DatabaseSimpleQueryBuilder query() {
        return this;
    }

    private String createSelectStatement() {
        return "select *" + fromClause() + whereClause() + orderByClause() + fetchClause();
    }

    protected String fromClause() {
        return " from " + table.getTableName();
    }

    protected String whereClause() {
        return conditions.isEmpty() ? "" : " where " + String.join(" and ", conditions);
    }

    protected String orderByClause() {
        return orderByClauses.isEmpty() ? "" : " order by " + String.join(", ", orderByClauses);
    }

    private String fetchClause() {
        return rowCount == null ? "" : " offset " + offset + " rows fetch first " + rowCount + " rows only";
    }

    private <T> T query(Connection connection, DatabaseResult.DatabaseResultMapper<T> resultMapper) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = new DatabaseResult(stmt, stmt.executeQuery())) {
                return resultMapper.apply(result);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"", (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }
}
