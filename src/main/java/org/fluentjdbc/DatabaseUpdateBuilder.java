package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabaseUpdateBuilder extends DatabaseStatement {

    private List<String> conditions;
    private DatabaseTable table;
    private List<Object> conditionParameters;
    private List<String> updateColumns;

    public DatabaseUpdateBuilder(DatabaseTable table, List<String> conditions, List<Object> conditionParameters) {
        this.table = table;
        this.conditions = conditions;
        this.conditionParameters = conditionParameters;
    }

    public void update(Connection connection, List<String> columns, List<Object> parameters) {
        this.updateColumns = columns;

        logger.debug(createUpdateStatement());
        try (PreparedStatement stmt = connection.prepareStatement(createUpdateStatement())) {
            int index = bindParameters(stmt, parameters);
            bindParameters(stmt, conditionParameters, index);

            stmt.executeUpdate();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    private String createUpdateStatement() {
        return "update " + table.getTableName()
            + " set " + String.join(",", updates(updateColumns))
            + " where " + String.join(" and ", conditions);
    }

    private static List<String> updates(List<String> columns) {
        List<String> result = new ArrayList<>();
        for (String column : columns) {
            result.add(column + " = ?");
        }
        return result;
    }


}
