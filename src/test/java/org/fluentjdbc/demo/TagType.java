package org.fluentjdbc.demo;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.DatabaseTableImpl;
import org.fluentjdbc.InsertMapper;
import org.fluentjdbc.Inserter;

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


    public TagType save(Connection connection) {
        this.id = TagType.tagTypesTable
            .newSaveBuilder("id", getId())
            .setField("name", getName())
            .execute(connection);
        return this;
    }

    public static List<TagType> list(Connection connection) {
        return tagTypesTable.listObjects(connection, createRowMapper());
    }

    public static TagType retrieve(Connection connection, Long id) {
        return tagTypesTable.where("id", id).singleObject(connection, createRowMapper());
    }

    private static RowMapper<TagType> createRowMapper() {
        return new RowMapper<TagType>() {

            @Override
            public TagType mapRow(DatabaseRow row) throws SQLException {
                TagType tagType = new TagType(row.getString("name"));
                tagType.setId(row.getLong("id"));
                return tagType;
            }
        };
    }

    public static void saveAll(List<TagType> tagTypes, Connection connection) {
        TagType.tagTypesTable.newBulkInserter(tagTypes, "name")
        .insert(new InsertMapper<TagType>() {
            @Override
            public void mapRow(Inserter inserter, TagType tagType) throws SQLException {
                inserter.setField("name", tagType.getName());
            }
        }).execute(connection);
    }


}
