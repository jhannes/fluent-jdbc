package org.fluentjdbc;

import org.joda.time.DateTime;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class DatabaseTableWithTimestamps extends DatabaseTableImpl implements DatabaseTable {

    public DatabaseTableWithTimestamps(String tableName) {
        super(tableName);
    }

    @Override
    public DatabaseInsertBuilder insert() {
        DateTime now = DateTime.now();
        return super.insert()
            .setField("updated_at", now)
            .setField("created_at", now);
    }


    @Override
    public DatabaseUpdateBuilder update() {
        return super.update().setField("updated_at", DateTime.now());
    }
}
