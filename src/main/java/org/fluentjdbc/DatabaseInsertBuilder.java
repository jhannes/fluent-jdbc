package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseInsertBuilder extends DatabaseStatement {

    private List<String> fieldNames = new ArrayList<>();
    private List<Object> parameters = new ArrayList<>();
    private DatabaseTable table;

    public DatabaseInsertBuilder(DatabaseTable table) {
        this.table = table;
    }

    public DatabaseInsertBuilder setField(String fieldName, @Nullable Object parameter) {
        this.fieldNames.add(fieldName);
        this.parameters.add(parameter);
        return this;
    }

    public DatabaseInsertBuilder setFields(List<String> fieldNames, List<Object> parameters) {
        this.fieldNames.addAll(fieldNames);
        this.parameters.addAll(parameters);
        return this;
    }

    public void execute(Connection connection) {
        logger.debug(createInsertStatement());
        try (PreparedStatement stmt = connection.prepareStatement(createInsertStatement())) {
            bindParameters(stmt, parameters);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public long generateKeyAndInsert(Connection connection) {
        logger.debug(createInsertStatement());
        try (PreparedStatement stmt = connection.prepareStatement(createInsertStatement(), Statement.RETURN_GENERATED_KEYS)) {
            bindParameters(stmt, parameters);
            stmt.executeUpdate();

            return getGeneratedKey(stmt);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    private String createInsertStatement() {
        String query = "insert into " + table.getTableName() +
                " (" + String.join(",", fieldNames)
                + ") values ("
                + String.join(",", repeat("?", fieldNames.size())) + ")";
        return query;
    }


    private long getGeneratedKey(PreparedStatement stmt) throws SQLException {
        try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
            if (!generatedKeys.next()) {
                throw new RuntimeException("Uh oh");
            }
            return generatedKeys.getLong(1);
        }
    }
}
