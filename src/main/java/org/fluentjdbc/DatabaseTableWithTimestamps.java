package org.fluentjdbc;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Instant;

/**
 * {@link DatabaseTable} which automatically adds <code>created_at</code> and <code>updated_at</code>
 * columns on {@link #insert()} and <code>updated_at</code> on {@link #update()}
 */
@ParametersAreNonnullByDefault
public class DatabaseTableWithTimestamps extends DatabaseTableImpl {

    public DatabaseTableWithTimestamps(String tableName, DatabaseStatementFactory factory) {
        super(tableName, factory);
    }

    public DatabaseTableWithTimestamps(String tableName) {
        super(tableName);
    }

    /**
     * Creates a {@link DatabaseInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement,
     * <strong>adding <code>created_at</code> and <code>updated_at</code> to the list of fields</strong>
     */
    @Override
    public DatabaseInsertBuilder insert() {
        Instant now = Instant.now();
        return super.insert()
            .setField("updated_at", now)
            .setField("created_at", now);
    }

    /**
     * Creates a {@link DatabaseBulkInsertBuilder} object to fluently generate a <code>INSERT ...</code> statement
     * for a list of objects and automatically add <code>created_at</code> and <code>updated_at</code> values. Example:
     *
     * <pre>
     *     public void saveAll(List&lt;TagType&gt; tagTypes, Connection connection) {
     *         tagTypesTable.bulkInsert(tagTypes)
     *             .setField("name", TagType::getName)
     *             .generatePrimaryKeys("id", TagType::setId)
     *             .execute(connection);
     *     }
     * </pre>
     */
    @Override
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(@Nonnull Iterable<T> objects) {
        return super.bulkInsert(objects)
                .setField("updated_at", t -> Instant.now())
                .setField("created_at", t -> Instant.now());
    }

    /**
     * Creates a {@link DatabaseBulkUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement
     * for a list of objects. Example:
     *
     * <pre>
     *     public void updateAll(List&lt;TagType&gt; tagTypes, Connection connection) {
     *         tagTypesTable.bulkUpdate(tagTypes)
     *              .where("id", TagType::getId)
     *              .setField("name", TagType::getName)
     *              .execute(connection);
     *     }
     * </pre>
     */
    @Override
    public <T> DatabaseBulkUpdateBuilder<T> bulkUpdate(@Nonnull Iterable<T> objects) {
        return super.bulkUpdate(objects).setField("updated_at", t -> Instant.now());
    }

    /**
     * Creates a {@link DatabaseUpdateBuilder} object to fluently generate a <code>UPDATE ...</code> statement,
     * <strong>adding <code>updated_at</code> to the list of fields</strong>
     */
    @Override
    public DatabaseUpdateBuilder update() {
        return super.update().setField("updated_at", Instant.now());
    }
}
