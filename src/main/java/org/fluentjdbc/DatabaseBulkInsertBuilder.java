package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


public class DatabaseBulkInsertBuilder<T> extends DatabaseStatement {

    private DatabaseTable table;
    private List<T> objects;
    private Map<String, Function<T, Object>> fields = new LinkedHashMap<>();

    public DatabaseBulkInsertBuilder(DatabaseTable table, List<T> objects) {
        this.table = table;
        this.objects = objects;
    }

    public DatabaseBulkInsertBuilder<T> setField(String fieldName, Function<T, Object> transformer) {
        fields.put(fieldName, transformer);
        return this;
    }

    public void execute(Connection connection) {
        String insertStatement = createInsertSql(table.getTableName(), fields.keySet());
        try (PreparedStatement statement = connection.prepareStatement(insertStatement)) {
            for (T object : objects) {
                int columnIndex = 1;
                for (Function<T, Object> f : fields.values()) {
                    bindParameter(statement, columnIndex++, f.apply(object));
                }
                statement.addBatch();
            }

            statement.executeBatch();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }

    }

}
