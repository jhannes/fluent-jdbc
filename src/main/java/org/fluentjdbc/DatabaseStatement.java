package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Allows the execution of arbitrary SQL statements with parameters and returns the ResultSet.
 * Will convert parameters to the statement using {@link #bindParameter(PreparedStatement, int, Object)},
 * which supports many more data types than JDBC supports natively. Returns the result via
 * {@link #list(Connection, DatabaseResult.RowMapper)}, {@link #singleObject(Connection, DatabaseResult.RowMapper)}
 * and {@link #stream(Connection, DatabaseResult.RowMapper)}, which uses {@link DatabaseRow} to convert
 * ResultSet types.
 */
@ParametersAreNonnullByDefault
public class DatabaseStatement {

    @FunctionalInterface
    public interface PreparedStatementFunction<T> {
        T apply(PreparedStatement stmt) throws SQLException;
    }


    protected static final Logger logger = LoggerFactory.getLogger(DatabaseStatement.class);
    private final String statement;
    private final Collection<?> parameters;
    private final DatabaseTableOperationReporter reporter;

    public DatabaseStatement(String statement, Collection<?> parameters, DatabaseTableOperationReporter reporter) {
        this.statement = statement;
        this.parameters = parameters;
        this.reporter = reporter;
    }

    /**
     * sets all parameters on the statement, calling {@link #bindParameter(PreparedStatement, int, Object)} to
     * convert each one
     */
    public static int bindParameters(PreparedStatement stmt, Collection<?> parameters) throws SQLException {
        return bindParameters(stmt, parameters, 1);
    }

    /**
     * sets all parameters on the statement, calling {@link #bindParameter(PreparedStatement, int, Object)} to
     * convert each one
     */
    public static int bindParameters(PreparedStatement stmt, Collection<?> parameters, int start) throws SQLException {
        int index = start;
        for (Object parameter : parameters) {
            bindParameter(stmt, index++, parameter);
        }
        return index;
    }

    /**
     * Calls the correct {@link PreparedStatement} <code>setXXX</code> method based on the type of the parameter.
     * Supports {@link Instant}, {@link ZonedDateTime}, {@link OffsetDateTime}, {@link LocalDate}, {@link String},
     * {@link List} of String or Integer, {@link Enum}, {@link UUID}, {@link Double}
     */
    public static void bindParameter(PreparedStatement stmt, int index, @Nullable Object parameter) throws SQLException {
        if (parameter instanceof Instant) {
            stmt.setTimestamp(index, (Timestamp) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof ZonedDateTime) {
            stmt.setTimestamp(index, (Timestamp) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof OffsetDateTime) {
            stmt.setTimestamp(index, (Timestamp) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof LocalDate) {
            stmt.setDate(index, (Date) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof CharSequence) {
            stmt.setString(index, (String) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof Enum<?>) {
            stmt.setString(index, (String) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof Collection<?>) {
            //noinspection rawtypes
            Object[] elements = ((Collection) parameter).toArray();
            if (elements.length == 0) {
                stmt.setArray(index, stmt.getConnection().createArrayOf(null, elements));
            } else if (elements[0] instanceof Integer) {
                stmt.setArray(index, stmt.getConnection().createArrayOf("integer", elements));
            } else if (elements[0] instanceof String) {
                stmt.setArray(index, stmt.getConnection().createArrayOf("varchar", elements));
            } else {
                throw new IllegalArgumentException("Not supported: Arrays of " + elements[0].getClass());
            }
        } else if (parameter instanceof InputStream) {
            stmt.setBinaryStream(index, ((InputStream) parameter));
        } else if (parameter instanceof Reader) {
            stmt.setCharacterStream(index, ((Reader) parameter));
        } else {
            stmt.setObject(index, toDatabaseType(parameter, stmt.getConnection()));
        }
    }

    /**
     * Converts parameter to canonical database type.
     * Supports {@link Instant}, {@link ZonedDateTime}, {@link OffsetDateTime}, {@link LocalDate}, {@link String},
     * {@link Enum}, {@link UUID}, {@link Double}
     */
    public static Object toDatabaseType(@Nullable Object parameter, Connection connection) {
        if (parameter instanceof Instant) {
            return Timestamp.from((Instant) parameter);
        } else if (parameter instanceof ZonedDateTime) {
            return Timestamp.from(Instant.from((ZonedDateTime) parameter));
        } else if (parameter instanceof OffsetDateTime) {
            return Timestamp.from(Instant.from((OffsetDateTime) parameter));
        } else if (parameter instanceof LocalDate) {
            return Date.valueOf((LocalDate) parameter);
        } else if (parameter instanceof UUID) {
            if (isSqlServer(connection)) {
                return parameter.toString().toUpperCase();
            } else if (isOracle(connection)) {
                return parameter.toString();
            } else {
                return parameter;
            }
        } else if (parameter instanceof Double) {
            return BigDecimal.valueOf(((Number) parameter).doubleValue());
        } else if (parameter instanceof CharSequence) {
            return parameter.toString();
        } else if (parameter instanceof Enum<?>) {
            return parameter.toString();
        } else {
            return parameter;
        }
    }

    /**
     * Binds the parameters and calls {@link PreparedStatement#addBatch()}.
     *
     * @see #bindParameter(PreparedStatement, int, Object)
     */
    public static <T> void addBatch(PreparedStatement statement, Iterable<T> objects, Collection<Function<T, ?>> columnValueExtractors) throws SQLException {
        for (T object : objects) {
            int columnIndex = 1;
            for (Function<T, ?> f : columnValueExtractors) {
                bindParameter(statement, columnIndex++, f.apply(object));
            }
            statement.addBatch();
        }
    }

    /**
     * Returns true if the database connection is to SQL server
     */
    private static boolean isSqlServer(Connection connection) {
        return connection.getClass().getName().startsWith("net.sourceforge.jtds.jdbc") ||
               connection.getClass().getName().startsWith("com.microsoft.sqlserver.jdbc");
    }

    /**
     * Returns true if the database connection is to Oracle
     */
    private static boolean isOracle(Connection connection) {
        return connection.getClass().getName().startsWith("oracle.jdbc");
    }

    /**
     * Calls {@link Connection#prepareStatement(String)} with the statement,
     * {@link #bindParameters(PreparedStatement, Collection)}, converting each parameter in the process
     * and executes the statement
     */
    public int executeUpdate(Connection connection) {
        return execute(connection, PreparedStatement::executeUpdate);
    }

    /**
     * Create a string like <code>?, ?, ?</code> with the parameterCount number of '?'
     */
    public static String parameterString(int parameterCount) {
        StringBuilder parameterString = new StringBuilder("?");
        for (int i = 1; i < parameterCount; i++) {
            parameterString.append(", ?");
        }
        return parameterString.toString();
    }

    /**
     * Returns true if the object value equals the specified field name in the database. Converts
     * {@link #toDatabaseType(Object, Connection)} to decrease number of false positives
     */
    public static boolean dbValuesAreEqual(Object value, DatabaseRow row, String field, Connection connection) throws SQLException {
        Object canonicalValue = toDatabaseType(value, connection);
        Object dbValue;
        if (canonicalValue instanceof Timestamp) {
            dbValue = row.getTimestamp(field);
        } else if (canonicalValue instanceof Integer) {
            dbValue = row.getInt(field);
        } else {
            dbValue = row.getObject(field);
        }
        return Objects.equals(canonicalValue, toDatabaseType(dbValue, connection));
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
    public <T> SingleRow<T> singleObject(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return query(connection, result -> result.single(
                mapper,
                () -> new NoRowsReturnedException(statement, parameters),
                () -> new MultipleRowsReturnedException(statement, parameters)
        ));
    }

    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a list. Example:
     * <pre>
     *     List&lt;Instant&gt; creationTimes = table.where("status", status).list(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    public <OBJECT> List<OBJECT> list(Connection connection, DatabaseResult.RowMapper<OBJECT> mapper) {
        return stream(connection, mapper).collect(Collectors.toList());
    }


    /**
     * Executes the <code>SELECT * FROM ...</code> statement and calls back to
     * {@link DatabaseResult.RowConsumer} for each returned row
     */
    public void forEach(Connection connection, DatabaseResult.RowConsumer consumer) {
        query(connection, result -> {
            result.forEach(consumer);
            return null;
        });
    }


    /**
     * Execute the query and map each return value over the {@link DatabaseResult.RowMapper} function to return a stream. Example:
     * <pre>
     *     table.where("status", status).stream(row -&gt; row.getInstant("created_at"))
     * </pre>
     */
    public <OBJECT> Stream<OBJECT> stream(Connection connection, DatabaseResult.RowMapper<OBJECT> mapper) {
        long startTime = System.currentTimeMillis();
        try {
            logger.trace(statement);
            PreparedStatement stmt = connection.prepareStatement(statement);
            bindParameters(stmt, parameters);
            DatabaseResult result = new DatabaseResult(stmt, stmt.executeQuery());
            return result.stream(mapper, statement);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            reporter.reportQuery(statement, System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Calls {@link Connection#prepareStatement(String)} with the statement,
     * {@link #bindParameters(PreparedStatement, Collection)}, converting each parameter in the process
     * and executes the argument function with the statement
     */
    public <T> T execute(Connection connection, PreparedStatementFunction<T> f) {
        long startTime = System.currentTimeMillis();
        logger.trace(statement);
        try (PreparedStatement stmt = connection.prepareStatement(statement)) {
            bindParameters(stmt, parameters);
            return f.apply(stmt);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            reporter.reportQuery(statement, System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Calls {@link Connection#prepareStatement(String, String[])} with the statement and columnNames,
     * {@link #bindParameters(PreparedStatement, Collection)}, converting each parameter in the process
     * and executes the argument function with the statement
     */
    public <T> T execute(Connection connection, PreparedStatementFunction<T> f, String[] columnNames) {
        long startTime = System.currentTimeMillis();
        logger.trace(statement);
        try (PreparedStatement stmt = connection.prepareStatement(statement, columnNames)) {
            bindParameters(stmt, parameters);
            return f.apply(stmt);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            reporter.reportQuery(statement, System.currentTimeMillis() - startTime);
        }
    }

    public <T> T query(Connection connection, DatabaseResult.DatabaseResultMapper<T> resultMapper) {
        return execute(connection, stmt -> {
            try (DatabaseResult result = new DatabaseResult(stmt, stmt.executeQuery())) {
                return resultMapper.apply(result);
            }
        });
    }

}
