package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.fluentjdbc.AbstractDatabaseTest.createTable;
import static org.fluentjdbc.AbstractDatabaseTest.dropTableIfExists;

public class DbContextSyncBuilderTest {

    private final Map<String, String> replacements;

    @Rule
    public final DbContextRule dbContext;

    private final DbContextTable table;

    private static final String CREATE_TABLE =
            "create table sync_test (id ${UUID} primary key, name varchar(200) not null, amount DECIMAL(20,2), updated_at ${DATETIME} not null, created_at ${DATETIME} not null)";

    public DbContextSyncBuilderTest() {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected DbContextSyncBuilderTest(DataSource dataSource, Map<String, String> replacements) {
        this.replacements = replacements;
        dbContext = new DbContextRule(dataSource);
        table = dbContext.tableWithTimestamps("sync_test");
    }

    @Before
    public void setupDatabase() {
        dropTableIfExists(dbContext.getThreadConnection(), "sync_test");
        createTable(dbContext.getThreadConnection(), CREATE_TABLE, replacements);
    }


    @Test
    public void shouldUpdateBigDecimal() {
        Map<String, Object> object = createObject("test name", BigDecimal.valueOf(10.25));

        sync(Collections.singletonList(object));

        object.put("amount", BigDecimal.valueOf(2.25));
        sync(Collections.singletonList(object));

        assertThat(table.where("id", object.get("id")).singleObject(row -> row.getBigDecimal("amount")).get())
                .isEqualTo(BigDecimal.valueOf(2.25));
    }

    @Test
    public void shouldAddAdditionalRow() {
        Map<String, Object> firstObject = createObject("test name", BigDecimal.valueOf(10.25));
        sync(Collections.singletonList(firstObject));

        Map<String, Object> secondObject = createObject("other name", BigDecimal.valueOf(2));
        sync(Collections.singletonList(secondObject));

        assertThat(table.query().list(row -> row.getString("name")))
                .contains(firstObject.get("name").toString(), secondObject.get("name").toString());
    }

    @Test
    public void internalMissingRowsAreEqual() {
        List<Map<String, Object>> entities = Collections.singletonList(createObject("test", BigDecimal.ONE));
        DbContextSyncBuilder<Map<String, Object>> syncBuilder = table.sync(entities).cacheExisting();

        assertThat(syncBuilder.areEqual(null, entities)).isFalse();
        assertThat(syncBuilder.areEqual(entities, null)).isFalse();
        assertThat(syncBuilder.areEqualLists(null, null)).isTrue();
        assertThat(syncBuilder.areEqualLists(Collections.emptyList(), entities)).isFalse();
    }

    public Map<String, Object> createObject(String name, BigDecimal value) {
        Map<String, Object> object = new HashMap<>();
        object.put("id", UUID.randomUUID());
        object.put("name", name);
        object.put("amount", value);
        return object;
    }

    public void sync(List<Map<String, Object>> entities) {
        table.sync(entities)
                .unique("id", o -> o.get("id"))
                .field("name", o -> o.get("name"))
                .field("amount", o -> o.get("amount"))
                .updateDiffering()
                .insertMissing();
    }

}
