package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Used to execute an arbitrary statement. Create with {@link DbContext#statement(String, List)}.
 *
 * <h2>Example</h2>
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

    public DbContextStatement(DbContext dbContext, String statement, List<Object> parameters) {
        this.dbContext = dbContext;
        this.statement = dbContext.getStatementFactory().newStatement("*", "*", statement, parameters);
    }

    /**
     * If the query returns no rows, returns {@link SingleRow#absent}, if exactly one row is returned, maps it and return it,
     * if more than one is returned, throws `IllegalStateException`
     *
     * @param mapper Function object to map a single returned row to an object
     * @return the mapped row if one row is returned, {@link SingleRow#absent} otherwise
     * @throws MultipleRowsReturnedException if more than one row was matched the query
     */
    @Nonnull
    @CheckReturnValue
    public <OBJECT> SingleRow<OBJECT> singleObject(DatabaseResult.RowMapper<OBJECT> mapper) {
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
     * {@link DatabaseStatement#bindParameters(PreparedStatement, Collection)} (PreparedStatement, List)}, converting each parameter in the process
     * and executes the statement
     */
    public int executeUpdate(Connection connection) {
        return statement.executeUpdate(connection);
    }
}
