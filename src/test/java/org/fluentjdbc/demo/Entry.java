package org.fluentjdbc.demo;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.DatabaseTableImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
public class Entry {

    public static final String CREATE_TABLE = "create table entries (id ${INTEGER_PK}, name varchar(50) not null)";

    public static final String CREATE_TAGGING_TABLE = "create table entry_taggings (id ${INTEGER_PK}, entry_id integer not null references entries(id), tag_id integer not null references tags(id))";

    @Getter @Setter
    private Long id;

    @Getter
    private final String name;

    public static DatabaseTable entryTaggingsTable = new DatabaseTableImpl("entry_taggings");

    public static DatabaseTable entriesTable = new DatabaseTableImpl("entries");

    public static Entry mapFromRow(DatabaseRow row) throws SQLException {
        Entry entry = new Entry(row.getString("name"));
        entry.setId(row.getLong("id"));
        return entry;
    }

    public Entry save(Connection connection, List<Tag> tags) throws SQLException {
        this.id = Entry.entriesTable
                .newSaveBuilder("id", getId())
                .setField("name", getName())
                .execute(connection)
                .getId();

        for (Tag tag : tags) {
            Entry.entryTaggingsTable
                .newSaveBuilder("id", null)
                .setField("tag_id", tag.getId())
                .setField("entry_id", getId())
                .execute(connection);
        }

        return this;
    }

    public static Entry retrieve(Connection connection, Long id) {
        return entriesTable.where("id", id).singleObject(connection, createRowMapper())
                .orElseThrow(() -> new IllegalArgumentException("Unknown id " + id));
    }

    private static RowMapper<Entry> createRowMapper() {
        return new RowMapper<Entry>() {
            @Override
            public Entry mapRow(DatabaseRow row) throws SQLException {
                return mapFromRow(row);
            }
        };
    }

}