package org.fluentjdbc;

import java.time.ZonedDateTime;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseTableWithTimestamps extends DatabaseTableImpl implements DatabaseTable {

    public DatabaseTableWithTimestamps(String tableName) {
        super(tableName);
    }

    @Override
    public DatabaseInsertBuilder insert() {
        ZonedDateTime now = ZonedDateTime.now();
        return super.insert()
            .setField("updated_at", now)
            .setField("created_at", now);
    }


    @Override
    public DatabaseUpdateBuilder update() {
        return super.update().setField("updated_at", ZonedDateTime.now());
    }
}
