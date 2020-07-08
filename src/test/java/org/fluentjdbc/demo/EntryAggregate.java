package org.fluentjdbc.demo;

import org.fluentjdbc.DatabaseResult;
import org.fluentjdbc.DatabaseRow;
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
    private final Entry entry;

    @Getter
    private final List<Tag> tags = new ArrayList<>();

    public EntryAggregate(Entry entry) {
        this.entry = entry;
    }

    public static EntryAggregate retrieve(Connection connection, Long id) {
        EntryAggregate result = new EntryAggregate(Entry.retrieve(connection, id));
        result.getTags().addAll(Tag.tagsTable
            .whereExpression("id in (select tag_id from entry_taggings where entry_id = ?)", id)
            .orderBy("name")
            .list(connection, Tag.createRowMapper()));
        return result;
    }

    public static List<EntryAggregate> list(Connection connection) {

        String sql = "select * from entries e inner join entry_taggings et on e.id = et.entry_id inner join tags t on et.tag_id = t.id order by e.id";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (DatabaseResult result = new DatabaseResult(stmt, stmt.executeQuery())) {
                List<EntryAggregate> entries = new ArrayList<>();

                EntryAggregate aggregate = null;
                while (result.next()) {
                    DatabaseRow row = result.row();
                    if (aggregate == null || !aggregate.getEntry().getId().equals(row.table("entries").getLong("id"))) {
                        aggregate = new EntryAggregate(Entry.mapFromRow(row.table("entries")));
                        entries.add(aggregate);
                    }
                    aggregate.getTags().add(Tag.createRowMapper().mapRow(row.table("tags")));
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
