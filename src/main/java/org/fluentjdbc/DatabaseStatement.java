package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
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
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods for generating queries
 * ({@link #createUpdateStatement(String, List, List)}, {@link #createInsertSql(String, Collection)},
 * converting parameters to {@link PreparedStatement} ({@link #bindParameter(PreparedStatement, int, Object)}
 * and comparing values ({@link #dbValuesAreEqual(Object, DatabaseRow, String, Connection)}.
 */
@ParametersAreNonnullByDefault
public class DatabaseStatement {
    protected static final Logger logger = LoggerFactory.getLogger(DatabaseStatement.class);
    private final String statement;
    private final List<Object> parameters;
    private final DatabaseTableOperationReporter reporter;

    public DatabaseStatement(String statement, List<Object> parameters, DatabaseTableOperationReporter reporter) {
        this.statement = statement;
        this.parameters = parameters;
        this.reporter = reporter;
    }

    /**
     * sets all parameters on the statement, calling {@link #bindParameter(PreparedStatement, int, Object)} to
     * convert each one
     */
    public static int bindParameters(PreparedStatement stmt, List<Object> parameters) throws SQLException {
        return bindParameters(stmt, parameters, 1);
    }

    /**
     * sets all parameters on the statement, calling {@link #bindParameter(PreparedStatement, int, Object)} to
     * convert each one
     */
    public static int bindParameters(PreparedStatement stmt, List<Object> parameters, int start) throws SQLException {
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
            return Timestamp.from((Instant)parameter);
        } else if (parameter instanceof ZonedDateTime) {
            return Timestamp.from(Instant.from((ZonedDateTime)parameter));
        } else if (parameter instanceof OffsetDateTime) {
            return Timestamp.from(Instant.from((OffsetDateTime)parameter));
        } else if (parameter instanceof LocalDate) {
            return Date.valueOf((LocalDate)parameter);
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
     * {@link #bindParameters(PreparedStatement, List)}, converting each parameter in the process
     * and executes the statement
     */
    public int executeUpdate(Connection connection) {
        long startTime = System.currentTimeMillis();
        logger.trace(statement);
        try (PreparedStatement stmt = connection.prepareStatement(statement)) {
            bindParameters(stmt, parameters);
            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            reporter.reportQuery(statement, System.currentTimeMillis()-startTime);
        }
    }

    /**
     * Creates String for
     * <code>INSERT INTO tableName (fieldName, fieldName, ...) VALUES (?, ?, ...)</code>
     */
    public static String createInsertSql(String tableName, Collection<String> fieldNames) {
        return "insert into " + tableName +
                " (" + String.join(",", fieldNames)
                + ") values ("
                + parameterString(fieldNames.size()) + ")";
    }

    /**
     * Creates String for
     * <code>UPDATE tableName SET updateField = ?, updateField = ? WHERE whereCondition AND whereCondition</code>
     */
    public static String createUpdateStatement(String tableName, List<String> updateFields, List<String> whereConditions) {
        return "update " + tableName
            + " set " + updateFields.stream().map(column -> column + " = ?").collect(Collectors.joining(","))
            + (whereConditions.isEmpty() ? "" : " where " + String.join(" and ", whereConditions));
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

    public <T> Optional<T> singleObject(Connection connection, DatabaseResult.RowMapper<T> mapper) {
        return query(connection, result -> result.single(mapper));
    }

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
            this.reporter.reportQuery(statement, System.currentTimeMillis()-startTime);
        }
    }

    public void forEach(Connection connection, DatabaseResult.RowConsumer consumer) {
        query(connection, result -> {
            result.forEach(consumer);
            return null;
        });
    }

    private <T> T query(Connection connection, DatabaseResult.DatabaseResultMapper<T> resultMapper) {
        long startTime = System.currentTimeMillis();
        logger.trace(statement);
        try(PreparedStatement stmt = connection.prepareStatement(statement)) {
            bindParameters(stmt, parameters);
            try (DatabaseResult result = new DatabaseResult(stmt, stmt.executeQuery())) {
                return resultMapper.apply(result);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            reporter.reportQuery(statement, System.currentTimeMillis()-startTime);
        }
    }

}
