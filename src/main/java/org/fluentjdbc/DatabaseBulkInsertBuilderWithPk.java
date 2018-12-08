package org.fluentjdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.fluentjdbc.util.ExceptionUtil;

public class DatabaseBulkInsertBuilderWithPk<T> extends DatabaseStatement {

    private DatabaseTable table;
    private Map<String, Function<T, Object>> fields;
    private BiConsumer<T, Long> primaryKeyCallback;
    private List<T> objects;

    public DatabaseBulkInsertBuilderWithPk(DatabaseTable table,
    Map<String, Function<T, Object>> fields,
    BiConsumer<T, Long> primaryKeyCallback,
    List<T> objects) {
        this(objects, table, fields, primaryKeyCallback);
    }

    public DatabaseBulkInsertBuilderWithPk(List<T> objects,
            DatabaseTable table,
            Map<String, Function<T, Object>> fields,
            BiConsumer<T, Long> primaryKeyCallback) {
        this.objects = objects;
        this.table = table;
        this.fields = fields;
        this.primaryKeyCallback = primaryKeyCallback;
    }

    public void execute(Connection connection) {
        String insertStatement = createInsertSql(table.getTableName(), fields.keySet());
        try (PreparedStatement statement = connection.prepareStatement(insertStatement, PreparedStatement.RETURN_GENERATED_KEYS)) {
            for (T object : objects) {
                int columnIndex = 1;
                for (Function<T, Object> f : fields.values()) {
                    bindParameter(statement, columnIndex++, f.apply(object));
                }
                statement.addBatch();
            }
            statement.executeBatch();

            ResultSet generatedKeys = statement.getGeneratedKeys();
            int i=0;
            for (T object : objects) {
                i++;
                if (!generatedKeys.next()) {
                    throw new IllegalStateException("Could not find generated keys for all rows: " + i);
                }
                primaryKeyCallback.accept(object, generatedKeys.getLong(1));
            }

        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }

    }
}
