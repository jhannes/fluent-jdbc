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

    @Nullable
    private Object idValue;

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

    public Object execute(Connection connection) {
        Object idValue = this.idValue;
        if (idValue == null) {
            return insertWithAutogeneratedKey(connection);
        } else {
            insertWithPregeneratedKey(connection);
            return idValue;
        }
    }

    private void insertWithPregeneratedKey(Connection connection) {
        assert idValue != null;
        logger.debug(createInsertStatement());
        try (PreparedStatement stmt = connection.prepareStatement(createInsertStatement())) {
            bindParameters(stmt, parameters);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    // TODO: This doesn't work for Android - we need to do select last_insert_rowid() explicitly (or update SQLDroid)
    private Object insertWithAutogeneratedKey(Connection connection) {
        logger.debug(createInsertStatement());
        try (PreparedStatement stmt = connection.prepareStatement(createInsertStatement(), Statement.RETURN_GENERATED_KEYS)) {
            bindParameters(stmt, parameters);
            stmt.executeUpdate();

            try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                generatedKeys.next();
                return generatedKeys.getLong(1);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    private String createInsertStatement() {
        String query = "insert into " + table.getTableName() +
                " (" + join(",", fieldNames)
                + ") values ("
                + join(",", repeat("?", fieldNames.size())) + ")";
        return query;
    }

    // TODO: This doesn't work for Android when idValue is null
    public DatabaseInsertBuilder setPrimaryKey(String idField, @Nullable Object idValue) {
        if (idValue != null) {
            this.idValue = idValue;
            setField(idField, idValue);
        }
        return this;
    }
}
