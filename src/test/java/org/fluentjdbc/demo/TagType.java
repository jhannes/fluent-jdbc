package org.fluentjdbc.demo;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.DatabaseTableImpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
@ToString
public class TagType {

    public enum AccessLevel {
        PUBLIC, LIMITED, PRIVATE
    }

    public static final String CREATE_TABLE = "create table tag_types (id ${INTEGER_PK}, name varchar(50) not null, valid_until DATE, created_at ${DATETIME}, is_favorite ${BOOLEAN}, access_level VARCHAR(20))";

    @Getter @Setter
    private Long id;

    @Getter
    private final String name;

    @Getter @Setter
    private LocalDate validUntil;

    @Getter @Setter
    private ZonedDateTime createdAt;

    @Getter @Setter
    private boolean isFavorite;

    @Getter @Setter
    private AccessLevel accessLevel;

    public static DatabaseTable tagTypesTable = new DatabaseTableImpl("tag_types");

    public TagType save(Connection connection) throws SQLException {
        this.id = TagType.tagTypesTable
            .newSaveBuilder("id", getId())
            .setField("name", getName())
            .setField("valid_until", getValidUntil())
            .setField("created_at", getCreatedAt())
            .setField("access_level", getAccessLevel())
            .setField("is_favorite", isFavorite())
            .execute(connection)
            .getId();
        return this;
    }

    public static void saveAll(List<TagType> tagTypes, Connection connection) {
        TagType.tagTypesTable.bulkInsert(tagTypes)
            .setField("name", TagType::getName)
            .generatePrimaryKeys(TagType::setId)
            .execute(connection);
    }

    public static List<TagType> list(Connection connection) {
        return tagTypesTable.orderBy("name").list(connection, createRowMapper());
    }

    public static Optional<TagType> retrieve(Connection connection, @NonNull Long id) {
        return tagTypesTable.where("id", id)
                .singleObject(connection, createRowMapper());
    }

    private static RowMapper<TagType> createRowMapper() {
        return row -> {
            TagType tagType = new TagType(row.getString("name"));
            tagType.setId(row.getLong("id"));
            tagType.setAccessLevel(row.getEnum(AccessLevel.class, "access_level"));
            tagType.setValidUntil(row.getLocalDate("valid_until"));
            tagType.setCreatedAt(row.getZonedDateTime("created_at"));
            tagType.setFavorite(row.getBoolean("is_favorite"));
            return tagType;
        };
    }

}
