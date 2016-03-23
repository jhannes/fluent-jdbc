package org.fluentjdbc.demo;

import org.fluentjdbc.DatabaseResult;
import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

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
        result.getTags().addAll(Tag.tagsTable
            .whereExpression("id in (select tag_id from entry_taggings where entry_id = ?)", id)
            .list(connection, Tag.createRowMapper()));
        return result;
    }

    public static List<EntryAggregate> list(Connection connection) {

        String sql = "select * from entries e inner join entry_taggings et on e.id = et.entry_id inner join tags t on et.tag_id = t.id order by e.id";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (DatabaseResult result = new DatabaseResult(stmt)) {
                List<EntryAggregate> entries = new ArrayList<>();
                EntryAggregate aggregate = null;
                while (result.next()) {
                    if (aggregate == null || aggregate.getEntry().getId() != result.table("entries").getLong("id")) {
                        aggregate = new EntryAggregate(Entry.mapFromRow(result.table("entries")));
                        entries.add(aggregate);
                    }


                    aggregate.getTags().add(Tag.createRowMapper().mapRow(result.table("tags")));
                }
                return entries;
            }
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
    }

    @Nullable
    public Tag getTagOfType(TagType tagType) {
        for (Tag tag : getTags()) {
            if (tag.getTagTypeId() == tagType.getId()) {
                return tag;
            }
        }
        return null;
    }

}
