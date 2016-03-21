package org.fluentjdbc.demo;

import org.fluentjdbc.DatabaseTableWithTimestamps;
import org.fluentjdbc.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
@EqualsAndHashCode
public class Tag {

    @Getter @Setter
    private Long id;

    @Getter
    private final String name;

    @Getter
    private final long tagTypeId;

    public static DatabaseTableWithTimestamps tagsTable = new DatabaseTableWithTimestamps("tags");

    public Tag(String name, TagType tagType) {
        this(name, tagType.getId());
    }

    public static Tag mapFromRow(Row row) throws SQLException {
        Tag tag = new Tag(row.getString("name"), row.getLong("type_id"));
        tag.setId(row.getLong("id"));
        return tag;
    }

    public Tag save(Connection connection) {
        this.id = Tag.tagsTable
                .newSaveBuilder("id", getId())
                .setField("type_id", getTagTypeId())
                .setField("name", getName())
                .execute(connection);
        return this;
    }

    public static List<Tag> listByTypes(Connection connection, TagType tagType) {
        return tagsTable.where("type_id", tagType.getId()).list(connection, Tag::mapFromRow);
    }

}
