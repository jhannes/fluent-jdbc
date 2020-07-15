package org.fluentjdbc;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.function.Function;

/**
 * Fluently generate a <code>INSERT ...</code> statement for a list of objects. Create with a list of object
 * and use {@link #setField(String, Function)} to pass in a function that will be called for each object
 * in the list.
 *
 * <p>Example:</p>
 *
 * <pre>
 *     public void saveAll(List&lt;TagType&gt; tagTypes) {
 *         tagTypesTable.bulkInsert(tagTypes)
 *             .setField("name", TagType::getName)
 *             .execute();
 *     }
 * </pre>
 */
public class DbContextBuildInsertBuilder<T> implements DatabaseBulkUpdatable<T, DbContextBuildInsertBuilder<T>> {

    private final DbTableContext tableContext;
    private final DatabaseBulkInsertBuilder<T> builder;

    public DbContextBuildInsertBuilder(DbTableContext tableContext, DatabaseBulkInsertBuilder<T> builder) {
        this.tableContext = tableContext;
        this.builder = builder;
    }

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk insert
     */
    @Override
    public DbContextBuildInsertBuilder<T> setField(String fieldName, Function<T, Object> transformer) {
        builder.setField(fieldName, transformer);
        return this;
    }

    /**
     * Adds a list function that will be called for each object to get the value
     * each of the corresponding parameters in the {@link PreparedStatement}
     */
    @Override
    public <VALUES extends List<?>> DbContextBuildInsertBuilder<T> setFields(List<String> fields, Function<T, VALUES> values) {
        builder.setFields(fields, values);
        return this;
    }

    /**
     * Executes <code>INSERT INTO table ...</code> and calls {@link PreparedStatement#addBatch()} for
     * each row
     *
     * @return the count of rows inserted
     */
    public int execute() {
        return builder.execute(tableContext.getConnection());
    }
}
