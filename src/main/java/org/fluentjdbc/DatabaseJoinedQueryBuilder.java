package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.fluentjdbc.DatabaseStatement.bindParameters;
import static org.fluentjdbc.DatabaseStatement.parameterString;

/**
 * {@link DatabaseQueryBuilder} used to generate joined queries using SQL-92 standard
 * <code>SELECT * FROM table1 a JOIN table2 b ON a.column = b.column</code>. To specify
 * columns for selection and tables for retrieval of columns, use {@link DatabaseTableAlias}
 * and {@link DatabaseColumnReference}.
 *
 * <p><strong>Only works well on JDBC drivers that implement {@link ResultSetMetaData#getTableName(int)}</strong>,
 * as this is used to calculate column indexes for aliased tables. This includes PostgreSQL, H2, HSQLDB, and SQLite,
 * but not Oracle or SQL Server.</p>
 *
 * <p>Pull requests are welcome for a substitute for SQL Server and Oracle.</p>
 *
 * <h3>Usage example:</h3>

 * <pre>
 * DatabaseTableAlias p = productsTable.alias("p");
 * DatabaseTableAlias o = ordersTable.alias("o");
 * return context
 *         .join(linesAlias.column("product_id"), p.column("product_id"))
 *         .join(linesAlias.column("order_id"), o.column("order_id"))
 *         .list(connection, row -&gt; new OrderLineEntity(
 *                 OrderRepository.toOrder(row.table(o)),
 *                 ProductRepository.toProduct(row.table(p)),
 *                 toOrderLine(row.table(linesAlias))
 *         ));
 * </pre>
 */
public class DatabaseJoinedQueryBuilder implements DatabaseQueryBuilder<DatabaseJoinedQueryBuilder>, DatabaseListableQueryBuilder {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseJoinedQueryBuilder.class);

    private final DatabaseTableAlias table;
    private final List<JoinedTable> joinedTables = new ArrayList<>();
    private final List<String> conditions = new ArrayList<>();
    private final List<Object> parameters = new ArrayList<>();
    private final List<String> orderByClauses = new ArrayList<>();

    public DatabaseJoinedQueryBuilder(DatabaseTableAlias table) {
        this.table = table;
    }

    /**
     * If you haven't called {@link #orderBy}, the results of {@link DatabaseListableQueryBuilder#list}
     * will be unpredictable. Call <code>unordered()</code> if you are okay with this.
     */
    @Override
    public DatabaseJoinedQueryBuilder unordered() {
        return this;
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    public DatabaseJoinedQueryBuilder orderBy(DatabaseColumnReference column) {
        return orderBy(column.getQualifiedColumnName());
    }

    /**
     * Adds an <code>order by</code> clause to the query. Needed in order to list results
     * in a predictable order.
     */
    @Override
    public DatabaseJoinedQueryBuilder orderBy(String orderByClause) {
        orderByClauses.add(orderByClause);
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DatabaseJoinedQueryBuilder where(String fieldName, @Nullable Object value) {
        return whereExpression(table.getAlias() + "." + fieldName + " = ?", value);
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    @Override
    public DatabaseJoinedQueryBuilder whereExpression(String expression, Object parameter) {
        whereExpression(expression);
        parameters.add(parameter);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpression("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    public DatabaseJoinedQueryBuilder whereExpressionWithMultipleParameters(String expression, Collection<?> parameters){
        whereExpression(expression);
        this.parameters.addAll(parameters);
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName in (?, ?, ?)</code>" to the query.
     * If the parameter list is empty, instead adds <code>WHERE fieldName &lt;&gt; fieldName</code>,
     * resulting in no rows being returned.
     */
    @Override
    public DatabaseJoinedQueryBuilder whereIn(String fieldName, Collection<?> parameters) {
        if (parameters.isEmpty()) {
            return whereExpression(fieldName + " <> " + fieldName);
        }
        whereExpression(fieldName + " IN (" + parameterString(parameters.size()) + ")");
        this.parameters.addAll(parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause
     */
    @Override
    public DatabaseJoinedQueryBuilder whereExpression(String expression) {
        conditions.add(expression);
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query unless value is null
     */
    @Override
    public DatabaseJoinedQueryBuilder whereOptional(String fieldName, @Nullable Object value) {
        if (value == null) return this;
        return where(fieldName, value);
    }

    /**
     * For each field adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DatabaseJoinedQueryBuilder whereAll(List<String> fields, List<Object> values) {
        fields.stream().map(s -> table.getAlias() + "." + s + " = ?").forEach(this.conditions::add);
        this.parameters.addAll(values);
        return this;
    }

    @Override
    public DatabaseJoinedQueryBuilder query() {
        return this;
    }

    /**
     * Adds an additional table to the join as an inner join. Inner joins require a matching row
     * in both tables and will leave out rows from one of the table where there is no corresponding
     * table in the other
     */
    public DatabaseJoinedQueryBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        joinedTables.add(new JoinedTable(a, b, "inner join"));
        return this;
    }

    /**
     * Adds an additional table to the join as a left join. Left join only require a matching row in
     * the first/left table. If there is no matching row in the second/right table, all columns are
     * returned as null in this table. When calling {@link DatabaseRow#table(DatabaseTableAlias)} on
     * the resulting row, <code>null</code> is returned
     */
    public DatabaseJoinedQueryBuilder leftJoin(DatabaseColumnReference a, DatabaseColumnReference b) {
        joinedTables.add(new JoinedTable(a, b, "left join"));
        return this;
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
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(connection, row -&gt; row.table(joinedTable).getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> Stream<T> stream(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return list(connection, mapper).stream();
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status)
     *          .list(connection, row -&gt; row.table(joinedTable).getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> List<T> list(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return query(connection, result -> result.list(mapper));
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
                return result.single(row -> row.getInt("count")).orElseThrow();
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"", (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    /**
     * Executes the resulting <code>SELECT * FROM table ... INNER JOIN table ...</code> statement and
     * calculates column indexes based on {@link ResultSetMetaData}
     */
    protected DatabaseResult createResult(PreparedStatement statement) throws SQLException {
        List<DatabaseTableAlias> aliases = new ArrayList<>();
        aliases.add(table);
        joinedTables.stream().map(JoinedTable::getAlias).forEach(aliases::add);

        Map<String, Integer> columnIndexes = new HashMap<>();
        Map<String, Map<String, Integer>> aliasColumnIndexes = new HashMap<>();
        aliases.forEach(t -> aliasColumnIndexes.put(t.getAlias().toUpperCase(), new HashMap<>()));
        int index = 0;

        ResultSet resultSet = statement.executeQuery();
        // Unfortunately, even though the database should know the alias for the each table, JDBC doesn't reveal it
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i=1; i<=metaData.getColumnCount(); i++) {
            while (!metaData.getTableName(i).equalsIgnoreCase(aliases.get(index).getTableName())) {
                index++;
                if (index == aliases.size()) {
                    throw new IllegalStateException("Failed to find table for column " + i + " (found " + aliasColumnIndexes + ") in " + aliases);
                }
            }
            String alias = aliases.get(index).getAlias().toUpperCase();
            String columnName = metaData.getColumnName(i).toUpperCase();
            if (aliasColumnIndexes.get(alias).containsKey(columnName)) {
                if (aliases.get(++index).getTableName().equalsIgnoreCase(metaData.getTableName(i))) {
                    alias = aliases.get(index).getAlias().toUpperCase();
                } else {
                    throw new IllegalStateException("Column twice in result " + alias + "." + columnName + ": " + aliasColumnIndexes);
                }
            }
            aliasColumnIndexes.get(alias).put(columnName, i);
            columnIndexes.putIfAbsent(columnName, i);
        }

        Map<DatabaseTableAlias, Integer> keys = new HashMap<>();
        for (JoinedTable table : joinedTables) {
            DatabaseColumnReference joinedTable = table.b;
            String tableAlias = joinedTable.getTableAlias().getAlias().toUpperCase();
            String columnAlias = joinedTable.getColumnName().toUpperCase();
            keys.put(joinedTable.getTableAlias(), aliasColumnIndexes.get(tableAlias).get(columnAlias));
        }
        return new DatabaseResult(statement, resultSet, columnIndexes, aliasColumnIndexes, keys);
    }

    private String createSelectStatement() {
        return "select *" + fromClause() + whereClause() + orderByClause();
    }

    protected String fromClause() {
        return " from " + table.getTableNameAndAlias() + " " +
                joinedTables.stream().map(JoinedTable::toSql).collect(Collectors.joining(" "));
    }

    protected String whereClause() {
        return conditions.isEmpty() ? "" : " where " + String.join(" and ", conditions);
    }

    protected String orderByClause() {
        return orderByClauses.isEmpty() ? "" : " order by " + String.join(", ", orderByClauses);
    }

    private <T> T query(Connection connection, DatabaseResult.DatabaseResultMapper<T> resultMapper) {
        long startTime = System.currentTimeMillis();
        String query = createSelectStatement();
        logger.trace(query);
        try(PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = createResult(stmt)) {
                return resultMapper.apply(result);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"", (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    private static class JoinedTable {
        private final DatabaseColumnReference a;
        private final DatabaseColumnReference b;
        private final String join;

        private JoinedTable(DatabaseColumnReference a, DatabaseColumnReference b, String join) {
            this.a = a;
            this.b = b;
            this.join = join;
        }

        public String toSql() {
            return join + " " + b.getTableNameAndAlias() + " on " + a.getQualifiedColumnName() + " = " + b.getQualifiedColumnName();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + toSql() + "]";
        }

        DatabaseTableAlias getAlias() {
            return b.getTableAlias();
        }
    }
}
