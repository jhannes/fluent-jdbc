package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
 * <h2>Usage example:</h2>
 *
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
public class DatabaseJoinedQueryBuilder implements
        DatabaseQueryBuilder<DatabaseJoinedQueryBuilder>,
        DatabaseListableQueryBuilder<DatabaseJoinedQueryBuilder> {

    private final DatabaseTable table;
    private final DatabaseTableAlias tableAlias;
    private final List<JoinedTable> joinedTables = new ArrayList<>();
    private final List<String> conditions = new ArrayList<>();
    private final List<Object> parameters = new ArrayList<>();
    private final List<String> orderByClauses = new ArrayList<>();
    private Integer offset;
    private Integer rowCount;

    public DatabaseJoinedQueryBuilder(DatabaseTable table, DatabaseTableAlias tableAlias) {
        this.table = table;
        this.tableAlias = tableAlias;
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
    @CheckReturnValue
    public DatabaseJoinedQueryBuilder orderBy(@Nonnull DatabaseColumnReference column) {
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
     * Adds <code>OFFSET ... ROWS FETCH ... ROWS ONLY</code> clause to the <code>SELECT</code>
     * statement. FETCH FIRST was introduced in
     * <a href="https://en.wikipedia.org/wiki/Select_%28SQL%29#Limiting_result_rows">SQL:2008</a>
     * and is supported by Postgresql 8.4, Oracle 12c, IBM DB2, HSQLDB, H2, and SQL Server 2012.
     */
    @Override
    public DatabaseJoinedQueryBuilder skipAndLimit(int offset, int rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
        return this;
    }

    /**
     * Adds "<code>WHERE fieldName = value</code>" to the query
     */
    @Override
    public DatabaseJoinedQueryBuilder where(String fieldName, @Nullable Object value) {
        return whereExpression(tableAlias.getAlias() + "." + fieldName + " = ?", value);
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereExpressionWithParameterList("created_at between ? and ?", List.of(earliestDate, latestDate))</code>
     */
    @Override
    public DatabaseJoinedQueryBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        conditions.add(expression);
        this.parameters.addAll(parameters);
        return this;
    }

    /**
     * Adds the expression to the WHERE-clause and all the values to the parameter list.
     * E.g. <code>whereColumnValues("json_column", "?::json", jsonString)</code>
     */
    @CheckReturnValue
    public DatabaseJoinedQueryBuilder whereColumnValuesEqual(String column, String expression, Collection<?> parameters) {
        conditions.add(column + " = " + expression);
        this.parameters.addAll(parameters);
        return this;
    }

    /**
     * Implemented as <code>return this</code> for compatibility purposes
     */
    @Override
    public DatabaseJoinedQueryBuilder query() {
        return this;
    }

    /**
     * Adds an additional table to the join as an inner join. Inner joins require a matching row
     * in both tables and will leave out rows from one of the table where there is no corresponding
     * table in the other
     */
    @CheckReturnValue
    public DatabaseJoinedQueryBuilder join(DatabaseColumnReference a, DatabaseColumnReference b) {
        joinedTables.add(new JoinedTable(a, b, "inner join"));
        return this;
    }

    /**
     * Adds an additional table to the join as an inner join with the initial table. Inner joins require that
     * all columns in both tables match and will leave out rows from one of the table where there is no corresponding
     * table in the other
     */
    @CheckReturnValue
    public DatabaseJoinedQueryBuilder join(List<String> leftFields, DatabaseTableAlias joinedTable, List<String> rightFields) {
        return join(tableAlias, leftFields, joinedTable, rightFields);
    }

    /**
     * Adds an additional table to the join as an inner join with the specified table. Inner joins require that
     * all columns  in both tables match and will leave out rows from one of the table where there is no
     * corresponding table in the other
     */
    @CheckReturnValue
    public DatabaseJoinedQueryBuilder join(DatabaseTableAlias leftTable, List<String> leftFields, DatabaseTableAlias joinedTable, List<String> rightFields) {
        joinedTables.add(new JoinedTable(leftTable, leftFields, joinedTable, rightFields, "inner join"));
        return this;
    }

    /**
     * Adds an additional table to the join as a left join. Left join only require a matching row in
     * the first/left table. If there is no matching row in the second/right table, all columns are
     * returned as null in this table. When calling {@link DatabaseRow#table(DatabaseTableAlias)} on
     * the resulting row, <code>null</code> is returned
     */
    @CheckReturnValue
    public DatabaseJoinedQueryBuilder leftJoin(DatabaseColumnReference a, DatabaseColumnReference b) {
        joinedTables.add(new JoinedTable(a, b, "left join"));
        return this;
    }

    /**
     * Adds an additional table to the join as a left join. Left join only require a matching row in
     * the first/left table. If there is no matching row in the second/right table, all columns are
     * returned as null in this table. When calling {@link DatabaseRow#table(DatabaseTableAlias)} on
     * the resulting row, <code>null</code> is returned
     */
    @CheckReturnValue
    public DatabaseJoinedQueryBuilder leftJoin(List<String> leftFields, DatabaseTableAlias joinedTable, List<String> rightFields) {
        joinedTables.add(new JoinedTable(tableAlias, leftFields, joinedTable, rightFields, "left join"));
        return this;
    }

    /**
     * If the query returns no rows, returns {@link SingleRow#absent}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param connection Database connection
     * @param mapper     Function object to map a single returned row to a object
     * @return the mapped row if one row is returned, {@link SingleRow#absent} otherwise
     * @throws MultipleRowsReturnedException if more than one row was matched the query
     */
    @Nonnull
    @Override
    public <T> SingleRow<T> singleObject(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return query(connection, result -> result.single(
                mapper,
                () -> new NoRowsReturnedException(createSelectStatement(), parameters),
                () -> new MultipleRowsReturnedException(createSelectStatement(), parameters)
        ));
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(connection, row -&gt; row.table(joinedTable).getInstant("created_at"))
     * </pre>
     */
    @Override
    public <T> Stream<T> stream(@Nonnull Connection connection, DatabaseResult.RowMapper<T> mapper) {
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
        String query = "select count(*) as count " + fromClause() + whereClause() + orderByClause();
        return table.newStatement("COUNT", query, parameters)
                .singleObject(connection, row -> row.getInt("count"))
                .orElseThrow();
    }

    /**
     * Executes the resulting <code>SELECT * FROM table ... INNER JOIN table ...</code> statement and
     * calculates column indexes based on {@link ResultSetMetaData}
     */
    @CheckReturnValue
    protected DatabaseResult createResult(@Nonnull PreparedStatement statement) throws SQLException {
        List<DatabaseTableAlias> aliases = new ArrayList<>();
        aliases.add(tableAlias);
        joinedTables.stream().map(JoinedTable::getAlias).forEach(aliases::add);

        Map<String, Integer> columnIndexes = new HashMap<>();
        Map<String, Map<String, Integer>> aliasColumnIndexes = new HashMap<>();
        aliases.forEach(t -> aliasColumnIndexes.put(t.getAlias().toUpperCase(), new HashMap<>()));
        int index = 0;

        ResultSet resultSet = statement.executeQuery();
        // Unfortunately, even though the database should know the alias for each table, JDBC doesn't reveal it
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
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
            String tableAlias = table.joinedTable.getAlias().toUpperCase();
            String columnAlias = table.rightFields.get(0).toUpperCase();
            keys.put(table.joinedTable, aliasColumnIndexes.get(tableAlias).get(columnAlias));
        }
        return new DatabaseResult(statement, resultSet, columnIndexes, aliasColumnIndexes, keys);
    }

    @CheckReturnValue
    protected String createSelectStatement() {
        return "select *" + fromClause() + whereClause() + orderByClause() + fetchClause();
    }

    @CheckReturnValue
    protected String fromClause() {
        return " from " + tableAlias.getTableNameAndAlias() + " " +
                joinedTables.stream().map(JoinedTable::toSql).collect(Collectors.joining(" "));
    }

    @CheckReturnValue
    protected String whereClause() {
        return conditions.isEmpty() ? "" : " where " + String.join(" and ", conditions);
    }

    @CheckReturnValue
    protected String orderByClause() {
        return orderByClauses.isEmpty() ? "" : " order by " + String.join(", ", orderByClauses);
    }

    @CheckReturnValue
    private String fetchClause() {
        return rowCount == null ? "" : " offset " + offset + " rows fetch first " + rowCount + " rows only";
    }

    private <T> T query(Connection connection, DatabaseResult.DatabaseResultMapper<T> resultMapper) {
        return table.newStatement("SELECT", createSelectStatement(), parameters).execute(connection, stmt -> {
            try (DatabaseResult result = createResult(stmt)) {
                return resultMapper.apply(result);
            }
        });
    }

    private static class JoinedTable {
        private final DatabaseTableAlias leftTable;
        private final List<String> leftFields;
        private final DatabaseTableAlias joinedTable;
        private final List<String> rightFields;
        private final String joinType;

        private JoinedTable(DatabaseColumnReference a, DatabaseColumnReference b, String joinType) {
            this(a.getTableAlias(), Collections.singletonList(a.getColumnName()), b.getTableAlias(), Collections.singletonList(b.getColumnName()), joinType);
        }

        public JoinedTable(DatabaseTableAlias leftTable, List<String> leftFields, DatabaseTableAlias joinedTable, List<String> rightFields, String joinType) {
            if (leftFields.size() != rightFields.size()) {
                throw new IllegalArgumentException("Column lists must have same length: " + leftFields + " != " + rightFields);
            }
            if (leftFields.isEmpty()) {
                throw new IllegalArgumentException("Must have at least one joined column");
            }
            this.leftTable = leftTable;
            this.leftFields = leftFields;
            this.joinedTable = joinedTable;
            this.rightFields = rightFields;
            this.joinType = joinType;
        }

        public String toSql() {
            List<String> joinParts = new ArrayList<>();
            for (int i = 0, leftFieldsSize = leftFields.size(); i < leftFieldsSize; i++) {
                joinParts.add(leftTable.getAlias() + "." + leftFields.get(i) + " = " + joinedTable.getAlias() + "." + rightFields.get(i));
            }

            return joinType + " " + joinedTable.getTableNameAndAlias()
                    + " on " + String.join(" and ", joinParts);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + toSql() + "]";
        }

        DatabaseTableAlias getAlias() {
            return joinedTable;
        }
    }
}
