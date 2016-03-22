package org.fluentjdbc.demo;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseTable;
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
public class TagType {

    public static final String CREATE_TABLE = "create table tag_types (id integer primary key auto_increment, name varchar not null)";

    @Getter @Setter
    private Long id;

    @Getter
    private final String name;

    public static DatabaseTable tagTypesTable = new DatabaseTableImpl("tag_types");


    public static TagType mapFromRow(DatabaseRow row) throws SQLException {
        TagType tagType = new TagType(row.getString("name"));
        tagType.setId(row.getLong("id"));
        return tagType;
    }


    public TagType save(Connection connection) {
        this.id = TagType.tagTypesTable
            .newSaveBuilder("id", getId())
            .setField("name", getName())
            .execute(connection);
        return this;
    }

    public static List<TagType> list(Connection connection) {
        return tagTypesTable.listObjects(connection, TagType::mapFromRow);
    }


    public static TagType retrieve(Connection connection, Long id) {
        return tagTypesTable.where("id", id).singleObject(connection, TagType::mapFromRow);
    }
}
