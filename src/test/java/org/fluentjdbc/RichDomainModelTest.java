package org.fluentjdbc;

import org.fluentjdbc.demo.Entry;
import org.fluentjdbc.demo.EntryAggregate;
import org.fluentjdbc.demo.Tag;
import org.fluentjdbc.demo.TagType;
import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class RichDomainModelTest extends AbstractDatabaseTest {

    public final Connection connection;
    private final Random random = new Random();
    private boolean databaseSupportsResultSetMetadataTableName = true;

    public RichDomainModelTest() throws SQLException {
        this(H2TestDatabase.createConnection(), H2TestDatabase.REPLACEMENTS);
    }

    protected RichDomainModelTest(Connection connection, Map<String, String> replacements) {
        super(replacements);
        this.connection = connection;
    }

    @Before
    public void createTables() {
        dropTableIfExists(connection, "entry_taggings");
        dropTableIfExists(connection, "entries");
        dropTableIfExists(connection, "tags");
        dropTableIfExists(connection, "tag_types");
        createTable(connection, TagType.CREATE_TABLE);
        createTable(connection, Tag.CREATE_TABLE);
        createTable(connection, Entry.CREATE_TABLE);
        createTable(connection, Entry.CREATE_TAGGING_TABLE);
    }

    @Test
    public void shouldRetrieveSimpleObject() throws SQLException {
        TagType tagType = sampleTagType().save(connection);

        assertThat(TagType.retrieve(connection, tagType.getId())).get()
            .isEqualToComparingFieldByField(tagType);
    }

    private TagType sampleTagType() {
        TagType tagType = new TagType("size");
        tagType.setValidUntil(LocalDate.now().plusDays(random.nextInt(365)));
        tagType.setCreatedAt(ZonedDateTime.now().minusMinutes(10).withNano(0));
        tagType.setFavorite(random.nextBoolean());
        tagType.setAccessLevel(pickOne(TagType.AccessLevel.values()));
        return tagType;
    }

    private <T> T pickOne(T[] options) {
        return options[random.nextInt(options.length)];
    }

    @Test
    public void shouldReturnList() throws SQLException {
        TagType sizeTagType = sampleTagType().save(connection);
        TagType colorTagType = new TagType("color").save(connection);

        assertThat(TagType.list(connection))
            .extracting("name")
            .contains(sizeTagType.getName(), colorTagType.getName());
    }

    @Test
    public void shouldRetrieveJoinedObjects() throws SQLException {
        TagType sizeTagType = sampleTagType().save(connection);
        TagType colorTagType = new TagType("color").save(connection);

        Tag astronomicalTag = new Tag("astronomical", sizeTagType).save(this.connection);
        Tag orangeTag = new Tag("orange", colorTagType).save(this.connection);

        Entry entry = new Entry("Sun").save(this.connection, Arrays.asList(astronomicalTag, orangeTag));

        EntryAggregate entryAggregate = EntryAggregate.retrieve(connection, entry.getId());

        assertThat(entryAggregate.getTags()).extracting("name").containsExactly("astronomical", "orange");
        assertThat(entryAggregate.getTags()).extracting("tagTypeId")
            .contains(sizeTagType.getId());
    }

    protected void databaseDoesNotSupportResultSetMetadataTableName() {
        databaseSupportsResultSetMetadataTableName = false;
    }

    @Test
    public void shouldGroupEntriesByTagTypes() throws SQLException {
        Assume.assumeTrue("Database vendor does not support ResultSetMetadata.getTableName",
                databaseSupportsResultSetMetadataTableName);

        TagType sizeTagType = sampleTagType().save(connection);
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
            .extracting("name")
            .containsOnly("Blueberry", "Balloon");
    }

    @Test
    public void shouldBulkInsert() {
        List<TagType> tagTypes = Arrays.asList(new TagType("a"), new TagType("b"), new TagType("c"));

        TagType.saveAll(tagTypes, connection);

        for (TagType tagType : tagTypes) {
            assertThat(TagType.retrieve(connection, tagType.getId())).get()
                .isEqualToComparingFieldByField(tagType);
        }

        assertThat(TagType.list(connection))
            .extracting("name")
            .contains("a", "b", "c");
    }


    private Map<Tag, Map<Tag, List<Entry>>> groupEntries(TagType primaryTagType, TagType secondaryTagType) {
        HashMap<Tag, Map<Tag, List<Entry>>> result = new HashMap<>();

        for (EntryAggregate entryAggregate : EntryAggregate.list(connection)) {
            Tag primaryTag = entryAggregate.getTagOfType(primaryTagType);
            Tag secondaryTag = entryAggregate.getTagOfType(secondaryTagType);

            if (!result.containsKey(primaryTag)) {
                result.put(primaryTag, new HashMap<>());
            }
            if (!result.get(primaryTag).containsKey(secondaryTag)) {
                result.get(primaryTag).put(secondaryTag, new ArrayList<>());
            }

            result.get(primaryTag).get(secondaryTag).add(entryAggregate.getEntry());
        }

        return result;
    }



}
