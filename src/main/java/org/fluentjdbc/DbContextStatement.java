package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Used to execute an arbitrary statement. Create with {@link DbContext#statement(String, List)}.
 *
 * <h3>Example</h3>
 *
 * <pre>
 * dbContext.statement(
 *      "update orders set quantity = quantity + 1 WHERE customer_id = ?",
 *      Arrays.asList(customerId)
 * ).executeUpdate();
 * </pre>
 */
public class DbContextStatement {
    private final DbContext dbContext;
    private final DatabaseStatement statement;

    public DbContextStatement(DbContext dbContext, String statement, List<Object> parameters, DatabaseTableOperationReporter reporter) {
        this.dbContext = dbContext;
        this.statement = new DatabaseStatement(statement, parameters, reporter);
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
    @CheckReturnValue
    public <OBJECT> Optional<OBJECT> singleObject(DatabaseResult.RowMapper<OBJECT> mapper) {
        return statement.singleObject(dbContext.getThreadConnection(), mapper);
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @CheckReturnValue
    public <OBJECT> Stream<OBJECT> stream(DatabaseResult.RowMapper<OBJECT> mapper) {
        return statement.stream(dbContext.getThreadConnection(), mapper);
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    @CheckReturnValue
    public <OBJECT> List<OBJECT> list(DatabaseResult.RowMapper<OBJECT> mapper) {
        return stream(mapper).collect(Collectors.toList());
    }

    /**
     * Calls {@link Connection#prepareStatement(String)} with the statement,
     * {@link DatabaseStatement#bindParameters(PreparedStatement, List)}, converting each parameter in the process
     * and executes the statement
     */
    public int executeUpdate(Connection connection) {
        return statement.executeUpdate(connection);
    }
}
