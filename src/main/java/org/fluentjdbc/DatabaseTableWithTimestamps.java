package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabaseTableWithTimestamps extends DatabaseStatement implements DatabaseTable {

    private String tableName;

    public DatabaseTableWithTimestamps(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public DatabaseSaveBuilder newSaveBuilder(String idField, Long id) {
        return new DatabaseSaveBuilder(tableName, idField, id);
    }

    @Override
    public DatabaseQueryBuilder where(String fieldName, Object value) {
        return new DatabaseQueryBuilder(tableName).where(fieldName, value);
    }

    @Override
    public <T> List<T> listObjects(Connection connection, RowMapper<T> mapper) {
        logger.debug("select * from " + tableName);
        try(PreparedStatement stmt = connection.prepareStatement("select * from " + tableName)) {
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

}
