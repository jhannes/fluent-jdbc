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
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
class DatabaseStatement {
    protected static Logger logger = LoggerFactory.getLogger(DatabaseStatement.class);

    protected int bindParameters(PreparedStatement stmt, List<Object> parameters) throws SQLException {
        return bindParameters(stmt, parameters, 1);
    }

    protected int bindParameters(PreparedStatement stmt, List<Object> parameters, int start) throws SQLException {
        int index = start;
        for (Object parameter : parameters) {
            bindParameter(stmt, index++, parameter);
        }
        return index;
    }

    protected void bindParameter(PreparedStatement stmt, int index, @Nullable Object parameter) throws SQLException {
        if (parameter instanceof Instant) {
            stmt.setTimestamp(index, (Timestamp) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof ZonedDateTime) {
            stmt.setTimestamp(index, (Timestamp) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof LocalDate) {
            stmt.setDate(index, (Date) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof CharSequence) {
            stmt.setString(index, (String) toDatabaseType(parameter, stmt.getConnection()));
        } else if (parameter instanceof Enum<?>) {
            stmt.setString(index, (String) toDatabaseType(parameter, stmt.getConnection()));
        } else {
            stmt.setObject(index, toDatabaseType(parameter, stmt.getConnection()));
        }
    }

    public static Object toDatabaseType(@Nullable Object parameter, Connection connection) {
        if (parameter instanceof Instant) {
            return Timestamp.from((Instant)parameter);
        } else if (parameter instanceof ZonedDateTime) {
            return Timestamp.from(Instant.from((ZonedDateTime)parameter));
        } else if (parameter instanceof LocalDate) {
            return Date.valueOf((LocalDate)parameter);
        } else if (parameter instanceof UUID && isSqlServer(connection)) {
            return parameter.toString().toUpperCase();
        } else if (parameter instanceof Double) {
            return BigDecimal.valueOf(((Number) parameter).doubleValue());
        } else if (parameter instanceof Temporal) {
            return parameter.toString();
        } else if (parameter instanceof CharSequence) {
            return parameter.toString();
        } else if (parameter instanceof Enum<?>) {
            return parameter.toString();
        } else {
            return parameter;
        }
    }

    protected <T> void addBatch(PreparedStatement statement, Iterable<T> objects, Collection<Function<T, ?>> columnValueExtractors) throws SQLException {
        for (T object : objects) {
            int columnIndex = 1;
            for (Function<T, ?> f : columnValueExtractors) {
                bindParameter(statement, columnIndex++, f.apply(object));
            }
            statement.addBatch();
        }
    }

    private static boolean isSqlServer(Connection connection) {
        return connection.getClass().getName().startsWith("net.sourceforge.jtds.jdbc") ||
                connection.getClass().getName().startsWith("com.microsoft.sqlserver.jdbc");
    }

    protected void executeUpdate(String query, List<Object> parameters, Connection connection) {
        long startTime = System.currentTimeMillis();
        logger.trace(query);
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            bindParameters(stmt, parameters);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        } finally {
            logger.debug("time={}s query=\"{}\"",
                    (System.currentTimeMillis()-startTime)/1000.0, query);
        }
    }

    protected String createInsertSql(String tableName, Collection<String> fieldNames) {
        return "insert into " + tableName +
                " (" + String.join(",", fieldNames)
                + ") values ("
                + parameterString(fieldNames.size()) + ")";
    }

    protected String createDeleteStatement(String tableName, List<String> whereConditions) {
        return "delete from " + tableName + " where "  + String.join(" and ", whereConditions);
    }

    protected String createUpdateStatement(String tableName, List<String> updateFields, List<String> whereConditions) {
        return "update " + tableName
            + " set " + updateFields.stream().map(column -> column + " = ?").collect(Collectors.joining(","))
            + (whereConditions.isEmpty() ? "" : " where " + String.join(" and ", whereConditions));
    }

    protected String parameterString(int parameterCount) {
        StringBuilder parameterString = new StringBuilder("?");
        for (int i = 1; i < parameterCount; i++) {
            parameterString.append(", ?");
        }
        return parameterString.toString();
    }

    protected boolean dbValuesAreEqual(Object a, Object b, Connection connection) {
        return Objects.equals(toDatabaseType(a, connection), toDatabaseType(b, connection));
    }
}
