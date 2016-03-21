package org.fluentjdbc.demo;

import org.fluentjdbc.DatabaseTableWithTimestamps;
import org.fluentjdbc.Row;

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

    @Getter @Setter
    private Long id;

    @Getter
    private final String name;

    public static DatabaseTableWithTimestamps entryTaggingsTable = new DatabaseTableWithTimestamps("entry_taggings");

    public static DatabaseTableWithTimestamps entriesTable = new DatabaseTableWithTimestamps("entries");

    public static Entry mapFromRow(Row row) throws SQLException {
        Entry entry = new Entry(row.getString("name"));
        entry.setId(row.getLong("id"));
        return entry;
    }

    public Entry save(Connection connection, List<Tag> tags) {
        this.id = Entry.entriesTable
                .newSaveBuilder("id", getId())
                .setField("name", getName())
                .execute(connection);

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
        return entriesTable.where("id", id).singleObject(connection, Entry::mapFromRow);
    }

}