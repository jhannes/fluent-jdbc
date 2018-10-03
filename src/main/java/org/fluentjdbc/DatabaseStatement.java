package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

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
        if (parameter instanceof DateTime) {
            stmt.setTimestamp(index, new Timestamp(((DateTime)parameter).getMillis()));
        } else if (parameter instanceof LocalDate) {
            stmt.setDate(index, new Date(((LocalDate)parameter).toDate().getTime()));
        } else if (parameter instanceof UUID && isSqlServer(stmt.getConnection())) {
            stmt.setObject(index, parameter.toString());
        } else {
            stmt.setObject(index, parameter);
        }
    }

    private boolean isSqlServer(Connection connection) {
        return connection.getClass().getName().startsWith("net.sourceforge.jtds.jdbc");
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

    String createInsertSql(String tableName, Collection<String> fieldNames) {
        return "insert into " + tableName +
                " (" + join(",", fieldNames)
                + ") values ("
                + join(",", repeat("?", fieldNames.size())) + ")";
    }

    protected static List<String> repeat(String string, int size) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(string);
        }
        return result;
    }

    protected static String join(String delimiter, Collection<String> strings) {
        StringBuilder result = new StringBuilder();
        for (String s : strings) {
            if (result.length() != 0) {
                result.append(delimiter);
            }
            result.append(s);
        }
        return result.toString();
    }
}
