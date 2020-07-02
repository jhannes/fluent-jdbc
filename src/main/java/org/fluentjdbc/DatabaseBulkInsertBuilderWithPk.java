package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class DatabaseBulkInsertBuilderWithPk<T> extends DatabaseStatement {

    private final String primaryKeyColumn;
    private final DatabaseTable table;
    private final List<String> updateFields;
    private final List<Function<T, ?>> updateParameters;
    private final BiConsumer<T, Long> primaryKeyCallback;
    private final Iterable<T> objects;

    public DatabaseBulkInsertBuilderWithPk(
            Iterable<T> objects,
            DatabaseTable table,
            List<String> updateFields,
            List<Function<T, ?>> updateParameters,
            String primaryKeyColumn,
            BiConsumer<T, Long> primaryKeyCallback
    ) {
        this.objects = objects;
        this.table = table;
        this.updateFields = updateFields;
        this.updateParameters = updateParameters;
        this.primaryKeyColumn = primaryKeyColumn;
        this.primaryKeyCallback = primaryKeyCallback;
    }

    public void execute(Connection connection) {
        String insertStatement = createInsertSql(table.getTableName(), updateFields);
        try (PreparedStatement statement = connection.prepareStatement(insertStatement, new String[] { primaryKeyColumn })) {
            addBatch(statement, objects, updateParameters);
            statement.executeBatch();

            ResultSet generatedKeys = statement.getGeneratedKeys();
            int i=0;
            for (T object : objects) {
                i++;
                if (!generatedKeys.next()) {
                    throw new IllegalStateException("Could not find generated keys for row: " + i);
                }
                primaryKeyCallback.accept(object, generatedKeys.getLong(1));
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }

    }
}
