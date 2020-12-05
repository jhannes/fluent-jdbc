package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

public class DatabaseSqlBuilder implements DatabaseQueryBuilder<DatabaseSqlBuilder>, DatabaseListableQueryBuilder<DatabaseSqlBuilder> {

    private final ArrayList<String> columns = new ArrayList<>();
    private final DatabaseTableReporter reporter;
    private String fromStatement;
    private final ArrayList<String> groupByClauses = new ArrayList<>();
    private final ArrayList<String> orderByClauses = new ArrayList<>();
    private Integer offset;
    private Integer rowCount;
    private final DatabaseWhereBuilder whereBuilder = new DatabaseWhereBuilder();

    public DatabaseSqlBuilder(DatabaseTableReporter reporter) {
        this.reporter = reporter;
    }

    public DatabaseSqlBuilder select(String... columns) {
        this.columns.addAll(Arrays.asList(columns));
        return this;
    }

    public DatabaseSqlBuilder from(String fromStatement) {
        this.fromStatement = fromStatement;
        return this;
    }

    @Override
    public DatabaseSqlBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        whereBuilder.whereExpressionWithParameterList(expression, parameters);
        return this;
    }

    public DatabaseSqlBuilder groupBy(String... groupByStatement) {
        groupByClauses.addAll(Arrays.asList(groupByStatement));
        return this;
    }

    @Override
    public DatabaseSqlBuilder unordered() {
        return this;
    }

    @Override
    public DatabaseSqlBuilder orderBy(String orderByClause) {
        orderByClauses.add(orderByClause);
        return this;
    }

    @Override
    public DatabaseSqlBuilder skipAndLimit(int offset, int rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
        return this;
    }

    @Nonnull
    @Override
    public <OBJECT> Optional<OBJECT> singleObject(Connection connection, DatabaseResult.RowMapper<OBJECT> mapper) {
        return getDatabaseStatement().singleObject(connection, mapper);
    }

    @Override
    public <OBJECT> Stream<OBJECT> stream(@Nonnull Connection connection, DatabaseResult.RowMapper<OBJECT> mapper) {
        return getDatabaseStatement().stream(connection, mapper);
    }

    @Override
    public void forEach(Connection connection, DatabaseResult.RowConsumer consumer) {
        getDatabaseStatement().forEach(connection, consumer);
    }

    @Override
    public int getCount(Connection connection) {
        String selectStatement = "select count(*) as count "
                + (" from " + fromStatement)
                + whereBuilder.whereClause()
                + (groupByClauses.isEmpty() ? "" : " group by " + String.join(", ", groupByClauses));
        return new DatabaseStatement(selectStatement, whereBuilder.getParameters(), reporter.operation("COUNT"))
                .singleObject(connection, row -> row.getInt("count"))
                .orElseThrow(() -> new RuntimeException("Should never happen"));
    }

    @Override
    public DatabaseSqlBuilder query() {
        return this;
    }

    @Nonnull
    private DatabaseStatement getDatabaseStatement() {
        return new DatabaseStatement(createSelectStatement(String.join(", ", columns)), whereBuilder.getParameters(), reporter.operation("SELECT"));
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
