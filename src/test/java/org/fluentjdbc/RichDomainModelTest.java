package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.fluentjdbc.demo.Entry;
import org.fluentjdbc.demo.EntryAggregate;
import org.fluentjdbc.demo.Tag;
import org.fluentjdbc.demo.TagType;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class RichDomainModelTest {

    public Connection connection;

    @Before
    public void openConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:" + getClass().getName());
        connection = DriverManager.getConnection(jdbcUrl);

        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table if exists entry_taggings");
            stmt.executeUpdate("drop table if exists entries");
            stmt.executeUpdate("drop table if exists tags");
            stmt.executeUpdate("drop table if exists tag_types");

            stmt.executeUpdate(preprocessCreateTable(connection, TagType.CREATE_TABLE));
            stmt.executeUpdate(preprocessCreateTable(connection, Tag.CREATE_TABLE));
            stmt.executeUpdate(preprocessCreateTable(connection, Entry.CREATE_TABLE));
            stmt.executeUpdate(preprocessCreateTable(connection, Entry.CREATE_TAGGING_TABLE));
        }
    }

    private static String preprocessCreateTable(Connection connection, String createTableStatement) throws SQLException {
        String productName = connection.getMetaData().getDatabaseProductName();
        if (productName.equals("SQLite")) {
            return createTableStatement.replaceAll("auto_increment", "autoincrement");
        } else {
            return createTableStatement;
        }
    }

    @Test
    public void shouldRetrieveSimpleObject() {
        TagType tagType = new TagType("size").save(connection);

        assertThat(TagType.retrieve(connection, tagType.getId()))
            .isEqualToComparingFieldByField(tagType);
    }

    @Test
    public void shouldReturnList() {
        TagType sizeTagType = new TagType("size").save(connection);
        TagType colorTagType = new TagType("color").save(connection);

        assertThat(TagType.list(connection))
            .extracting(TagType::getName)
            .contains(sizeTagType.getName(), colorTagType.getName());
    }


    @Test
    public void shouldRetrieveJoinedObjects() {
        TagType sizeTagType = new TagType("size").save(connection);
        TagType colorTagType = new TagType("color").save(connection);

        Tag astronomicalTag = new Tag("astronomical", sizeTagType).save(this.connection);
        Tag orangeTag = new Tag("orange", colorTagType).save(this.connection);

        Entry entry = new Entry("Sun").save(this.connection, Arrays.asList(astronomicalTag, orangeTag));

        EntryAggregate entryAggregate = EntryAggregate.retrieve(connection, entry.getId());

        assertThat(entryAggregate.getTags()).extracting(t -> t.getName()).contains("astronomical", "orange");
        assertThat(entryAggregate.getTags())
            .filteredOn(t -> t.getName().equals("astronomical"))
            .extracting(t -> t.getTagTypeId())
            .containsExactly(sizeTagType.getId());
    }


    @Test
    public void shouldGroupEntriesByTagTypes() {
        TagType sizeTagType = new TagType("size").save(connection);
        TagType colorTagType = new TagType("color").save(connection);

        Tag astronomical = new Tag("astronomical", sizeTagType).save(this.connection);
        Tag large = new Tag("large", sizeTagType).save(this.connection);
        Tag small = new Tag("small", sizeTagType).save(this.connection);
        Tag orange = new Tag("orange", colorTagType).save(this.connection);
        Tag blue = new Tag("blue", colorTagType).save(this.connection);
        Tag red = new Tag("red", colorTagType).save(this.connection);
        Tag green = new Tag("green", colorTagType).save(this.connection);

        new Entry("Sun").save(this.connection, Arrays.asList(astronomical, orange));
        new Entry("Neptune").save(this.connection, Arrays.asList(astronomical, blue));
        new Entry("Mars").save(this.connection, Arrays.asList(astronomical, red));
        new Entry("Basketball").save(this.connection, Arrays.asList(small, orange));
        new Entry("Blueberry").save(this.connection, Arrays.asList(small, blue));
        new Entry("Grape").save(this.connection, Arrays.asList(small, green));
        new Entry("Balloon").save(this.connection, Arrays.asList(small, blue));
        new Entry("Blue whale").save(this.connection, Arrays.asList(large, blue));

        Map<Tag, Map<Tag, List<Entry>>> entriesGroupedByColorAndSize = groupEntries(sizeTagType, colorTagType);

        assertThat(entriesGroupedByColorAndSize.get(astronomical).keySet()).containsOnly(orange, blue, red);
        assertThat(entriesGroupedByColorAndSize.get(small).get(blue))
            .extracting(Entry::getName)
            .containsOnly("Blueberry", "Balloon");
    }



    private Map<Tag, Map<Tag, List<Entry>>> groupEntries(TagType primaryTagType, TagType secondaryTagType) {
        HashMap<Tag, Map<Tag, List<Entry>>> result = new HashMap<>();

        for (EntryAggregate entryAggregate : EntryAggregate.list(connection)) {
            Tag primaryTag = entryAggregate.getTags().stream().filter(t -> t.getTagTypeId() == primaryTagType.getId()).findFirst().get();
            Tag secondaryTag = entryAggregate.getTags().stream().filter(t -> t.getTagTypeId() == secondaryTagType.getId()).findFirst().get();

            result.computeIfAbsent(primaryTag, tag -> new HashMap<>())
                .computeIfAbsent(secondaryTag, key -> new ArrayList<>())
                .add(entryAggregate.getEntry());
        }

        return result;
    }



}
