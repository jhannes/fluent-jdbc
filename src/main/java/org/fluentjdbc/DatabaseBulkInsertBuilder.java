package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DatabaseBulkInsertBuilder<T> extends DatabaseStatement {

    private DatabaseTable table;
    private Iterable<T> objects;
    private Map<String, Function<T, Object>> fields = new LinkedHashMap<>();

    DatabaseBulkInsertBuilder(DatabaseTable table, Iterable<T> objects) {
        this.table = table;
        this.objects = objects;
    }

    DatabaseBulkInsertBuilder(DatabaseTable table, Stream<T> objects) {
        this(table, objects.collect(Collectors.toList()));
    }

    public DatabaseBulkInsertBuilder<T> setField(String fieldName, Function<T, Object> transformer) {
        fields.put(fieldName, transformer);
        return this;
    }

    public <VALUES extends List<?>> DatabaseBulkInsertBuilder<T> setFields(List<String> fields, Function<T, VALUES> values) {
        for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
            int index = i;
            setField(fields.get(i), o -> values.apply(o).get(index));
        }

        return this;
    }

    public void execute(Connection connection) {
        String insertStatement = createInsertSql(table.getTableName(), fields.keySet());
        try (PreparedStatement statement = connection.prepareStatement(insertStatement)) {
            addBatch(statement, objects, fields.values());
            statement.executeBatch();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }

    }

    public DatabaseBulkInsertBuilderWithPk<T> generatePrimaryKeys(BiConsumer<T, Long> consumer) {
        return new DatabaseBulkInsertBuilderWithPk<>(objects, table, fields, consumer);
    }

}
