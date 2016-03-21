package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabaseQueryBuilder extends DatabaseStatement {

    private final String tableName;
    private List<String> conditions = new ArrayList<>();

    public DatabaseQueryBuilder(String tableName) {
        this.tableName = tableName;
    }

    public <T> List<T> list(Connection connection, RowMapper<T> mapper) {
        logger.debug(createSelectStatement());
        try(PreparedStatement stmt = connection.prepareStatement(createSelectStatement())) {
            bindParameters(stmt);
            try (ResultSet rs = stmt.executeQuery()) {
                List<T> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapper.mapRow(rs));
                }
                return result;
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public <T> T singleObject(Connection connection, RowMapper<T> mapper) {
        logger.debug(createSelectStatement());
        try(PreparedStatement stmt = connection.prepareStatement(createSelectStatement())) {
            bindParameters(stmt);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                T result = mapper.mapRow(rs);
                if (rs.next()) {
                    throw new RuntimeException("More than one row returned from " + createSelectStatement());
                }
                return result;
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public String singleString(Connection connection, String fieldName) {
        return singleObject(connection, row -> row.getString(fieldName));
    }


    private String createSelectStatement() {
        return "select * from " + tableName + " where " + String.join(" AND ", conditions);
    }

    public DatabaseQueryBuilder where(String fieldName, Object value) {
        conditions.add(fieldName + " = ?");
        parameters.add(value);
        return this;
    }



}
