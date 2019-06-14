package org.fluentjdbc.demo;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.DatabaseTableImpl;

@RequiredArgsConstructor
@ToString
public class TagType {

    public static final String CREATE_TABLE = "create table tag_types (id ${INTEGER_PK}, name varchar(50) not null, valid_until DATE)";

    @Getter @Setter
    private Long id;

    @Getter
    private final String name;

    @Getter @Setter
    private LocalDate validUntil;

    public static DatabaseTable tagTypesTable = new DatabaseTableImpl("tag_types");


    public TagType save(Connection connection) throws SQLException {
        this.id = TagType.tagTypesTable
            .newSaveBuilder("id", getId())
            .setField("name", getName())
            .setField("valid_until", getValidUntil())
            .execute(connection)
            .getId();
        return this;
    }

    public static void saveAll(List<TagType> tagTypes, Connection connection) {
        TagType.tagTypesTable.bulkInsert(tagTypes)
            .setField("name", t -> t.getName())
            .generatePrimaryKeys((t, id) -> t.setId(id))
            .execute(connection);
    }

    public static List<TagType> list(Connection connection) {
        return tagTypesTable.listObjects(connection, createRowMapper());
    }

    public static TagType retrieve(Connection connection, @NonNull Long id) {
        return tagTypesTable.where("id", id)
                .singleObject(connection, createRowMapper());
    }

    private static RowMapper<TagType> createRowMapper() {
        return new RowMapper<TagType>() {

            @Override
            public TagType mapRow(DatabaseRow row) throws SQLException {
                TagType tagType = new TagType(row.getString("name"));
                tagType.setId(row.getLong("id"));
                tagType.setValidUntil(row.getLocalDate("valid_until"));
                return tagType;
            }
        };
    }

}
