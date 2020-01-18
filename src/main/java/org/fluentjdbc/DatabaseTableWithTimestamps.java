package org.fluentjdbc;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Instant;
import java.util.List;

@ParametersAreNonnullByDefault
public class DatabaseTableWithTimestamps extends DatabaseTableImpl {

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
    public <T> DatabaseBulkInsertBuilder<T> bulkInsert(Iterable<T> objects) {
        return super.bulkInsert(objects)
                .setField("updated_at", t -> Instant.now())
                .setField("created_at", t -> Instant.now());
    }

    @Override
    public <T> DatabaseBulkUpdateBuilder<T> bulkUpdate(List<T> objects) {
        return super.bulkUpdate(objects).setField("updated_at", t -> Instant.now());
    }

    @Override
    public DatabaseUpdateBuilder update() {
        return super.update().setField("updated_at", Instant.now());
    }
}
