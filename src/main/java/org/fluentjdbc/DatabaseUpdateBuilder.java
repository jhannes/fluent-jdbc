package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseUpdateBuilder extends DatabaseStatement {

    private final DatabaseTable table;
    private final List<String> conditions;
    private final List<Object> conditionParameters;
    private final List<String> updateFields = new ArrayList<>();
    private final List<Object> updateValues = new ArrayList<>();

    public DatabaseUpdateBuilder(DatabaseTable table, List<String> conditions, List<Object> conditionParameters) {
        this.table = table;
        this.conditions = conditions;
        this.conditionParameters = conditionParameters;
    }

    public void update(Connection connection, List<String> columns, List<Object> parameters) {
        setFields(columns, parameters);
        execute(connection);
    }

    public DatabaseUpdateBuilder setFields(List<String> fields, List<Object> values) {
        this.updateFields.addAll(fields);
        this.updateValues.addAll(values);
        return this;
    }

    public void execute(Connection connection) {
        logger.debug(createUpdateStatement());
        try (PreparedStatement stmt = connection.prepareStatement(createUpdateStatement())) {
            int index = bindParameters(stmt, updateValues);
            bindParameters(stmt, conditionParameters, index);

            stmt.executeUpdate();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    private String createUpdateStatement() {
        return "update " + table.getTableName()
            + " set " + String.join(",", updates(updateFields))
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
