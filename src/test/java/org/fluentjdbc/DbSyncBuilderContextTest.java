package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class DbSyncBuilderContextTest {

    private final Map<String, String> replacements;

    @Rule
    public final DbContextRule dbContext;

    private final DbTableContext table;

    private static final String CREATE_TABLE =
            "create table sync_test (id ${UUID} primary key, name varchar(200) not null, value DECIMAL, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)";

    public DbSyncBuilderContextTest() {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected DbSyncBuilderContextTest(DataSource dataSource, Map<String, String> replacements) {
        this.replacements = replacements;
        dbContext = new DbContextRule(dataSource);
        table = dbContext.tableWithTimestamps("sync_test");
    }

    @Before
    public void setupDatabase() throws SQLException {
        try (Statement statement = dbContext.getThreadConnection().createStatement()) {
            dropTableIfExists(statement, "sync_test");
            statement.executeUpdate(AbstractDatabaseTest.preprocessCreateTable(CREATE_TABLE, replacements));
        }
    }

    protected void dropTableIfExists(Statement stmt, String tableName) {
        try {
            stmt.executeUpdate("drop table " + tableName);
        } catch(SQLException ignored) {
        }
    }


    @Test
    public void shouldUpdateBigDecimal() {
        Map<String, Object> object = createObject("testname", BigDecimal.valueOf(10.25));

        sync(Collections.singletonList(object));

        object.put("value", BigDecimal.valueOf(2.5));
        sync(Collections.singletonList(object));

        assertThat(table.where("id", object.get("id")).singleObject(row -> row.getBigDecimal("value")))
                .get().isEqualTo(BigDecimal.valueOf(2.5));
    }

    @Test
    public void shouldAddAdditionalRow() {
        Map<String, Object> firstObject = createObject("testname", BigDecimal.valueOf(10.25));
        sync(Collections.singletonList(firstObject));

        Map<String, Object> secondObject = createObject("other name", BigDecimal.valueOf(2));
        sync(Collections.singletonList(secondObject));

        assertThat(table.query().list(row -> row.getString("name")))
                .contains(firstObject.get("name").toString(), secondObject.get("name").toString());
    }

    @Test
    public void internalMissingRowsAreEqual() {
        List<Map<String, Object>> entities = Collections.singletonList(createObject("test", BigDecimal.ONE));
        DbSyncBuilderContext<Map<String, Object>> syncContext = table.synch(entities).cacheExisting();

        assertThat(syncContext.areEqual(null, entities)).isFalse();
        assertThat(syncContext.areEqual(entities, null)).isFalse();
        assertThat(syncContext.areEqualLists(null, null)).isTrue();
        assertThat(syncContext.areEqualLists(Collections.emptyList(), entities)).isFalse();
    }

    public Map<String, Object> createObject(String name, BigDecimal value) {
        Map<String, Object> object = new HashMap<>();
        object.put("id", UUID.randomUUID());
        object.put("name", name);
        object.put("value", value);
        return object;
    }

    public void sync(List<Map<String, Object>> entities) {
        table.synch(entities)
                .unique("id", o -> o.get("id"))
                .field("name", o -> o.get("name"))
                .field("value", o -> o.get("value"))
                .updateDiffering()
                .insertMissing();
    }

}