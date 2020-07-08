package org.fluentjdbc.demo;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.DatabaseTableImpl;

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

    public static final String CREATE_TABLE = "create table tags (id ${INTEGER_PK}, name varchar(50) not null, type_id integer not null references tag_types(id))";

    @Getter @Setter
    private Long id;

    @Getter
    private final String name;

    @Getter
    private final long tagTypeId;

    public static final DatabaseTable tagsTable = new DatabaseTableImpl("tags");

    public Tag(String name, TagType tagType) {
        this(name, tagType.getId());
    }

    public Tag save(Connection connection) throws SQLException {
        this.id = Tag.tagsTable
                .newSaveBuilder("id", getId())
                .setField("type_id", getTagTypeId())
                .setField("name", getName())
                .execute(connection)
                .getId();
        return this;
    }

    public static List<Tag> listByTypes(Connection connection, TagType tagType) {
        return tagsTable.where("type_id", tagType.getId()).unordered().list(connection, createRowMapper());
    }

    static RowMapper<Tag> createRowMapper() {
        return new RowMapper<Tag>() {
            @Override
            public Tag mapRow(DatabaseRow row) throws SQLException {
                return mapFromRow(row);
            }
        };
    }

    static Tag mapFromRow(DatabaseRow row) throws SQLException {
        Tag tag = new Tag(row.getString("name"), row.getLong("type_id"));
        tag.setId(row.getLong("id"));
        return tag;
    }

}
