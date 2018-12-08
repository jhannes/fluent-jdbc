package org.fluentjdbc;

import java.time.Instant;
import java.util.List;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseTableWithTimestamps extends DatabaseTableImpl implements DatabaseTable {

    public DatabaseTableWithTimestamps(String tableName) {
        super(tableName);
    }

    @Override
    public DatabaseInsertBuilder insert() {
        Instant now = Instant.now();
        return super.insert()
            .setField("updated_at", now)
            .setField("created_at", now);
    }


    @Override
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(List<T> objects) {
        DatabaseBulkInsertBuilder<T> builder = super.bulkInsert(objects);
        builder.setField("updated_at", t -> Instant.now());
        builder.setField("created_at", t -> Instant.now());
        return builder;
    }

    @Override
    public DatabaseUpdateBuilder update() {
        return super.update().setField("updated_at", Instant.now());
    }
}
