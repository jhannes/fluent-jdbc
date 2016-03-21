package org.fluentjdbc.demo;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.ToString;

@ToString
public class EntryAggregate {

    @Getter
    private Entry entry;

    @Getter
    private List<Tag> tags = new ArrayList<>();

    public EntryAggregate(Entry entry) {
        this.entry = entry;
    }

    public static EntryAggregate retrieve(Connection connection, Long id) {
        EntryAggregate result = new EntryAggregate(Entry.retrieve(connection, id));

        for (Long tagId : Entry.entryTaggingsTable.where("entry_id", id).list(connection, row -> row.getLong("tag_id"))) {
            result.getTags().add(Tag.tagsTable.where("id", tagId).singleObject(connection, Tag::mapFromRow));
        }

        return result;
    }

    public static List<EntryAggregate> list(Connection connection) {
        ArrayList<EntryAggregate> result = new ArrayList<>();

        for (Entry entry : Entry.entriesTable.listObjects(connection, Entry::mapFromRow)) {
            EntryAggregate aggregate = new EntryAggregate(entry);

            for (Long tagId : Entry.entryTaggingsTable.where("entry_id", entry.getId()).list(connection, row -> row.getLong("tag_id"))) {
                aggregate.getTags().add(Tag.tagsTable.where("id", tagId).singleObject(connection, Tag::mapFromRow));
            }

            result.add(aggregate);
        }

        return result;
    }

}
