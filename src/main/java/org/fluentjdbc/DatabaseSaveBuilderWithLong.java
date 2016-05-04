package org.fluentjdbc;

public class DatabaseSaveBuilderWithLong extends DatabaseSaveBuilder<Long> {

    DatabaseSaveBuilderWithLong(DatabaseTable table, String idField, Long id) {
        super(table, idField, id);
    }

}
