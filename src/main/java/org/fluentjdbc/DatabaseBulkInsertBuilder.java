package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DatabaseBulkInsertBuilder<T> extends DatabaseStatement {

    private List<T> objects = new ArrayList<>();
    private InsertMapper<T> insertMapper;
    private List<String> fieldNames = new ArrayList<>();
    private DatabaseTable table;

    public DatabaseBulkInsertBuilder(DatabaseTable table) {
        this.table = table;
    }

    public DatabaseBulkInsertBuilder(DatabaseTable table, List<T> objects) {
        this(table);
        this.objects.addAll(objects);
    }

    public void addFieldNames(String... newFieldNames) {
        this.fieldNames.addAll(Arrays.asList(newFieldNames));
    }

    public void execute(Connection connection) {
        String insertStatement = "insert into " + table.getTableName() +
                    " (" + join(",", fieldNames)
                    + ") values ("
                    + join(",", repeat("?", fieldNames.size())) + ")";


        DateTime now = DateTime.now();
        try (PreparedStatement statement = connection.prepareStatement(insertStatement)) {
            for (T object : objects) {
                Inserter inserter = new Inserter(statement, fieldNames);
                insertMapper.mapRow(inserter, object);

                // HACK: There must be a better way of doing this!
                if (table instanceof DatabaseTableWithTimestamps) {
                    inserter.setField("updated_at", now);
                    inserter.setField("created_at", now);
                }
                statement.addBatch();
            }

            statement.executeBatch();
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    public DatabaseBulkInsertBuilder<T> insert(InsertMapper<T> insertMapper) {
        this.insertMapper = insertMapper;
        return this;
    }
}
