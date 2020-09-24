package org.fluentjdbc.usage.context;

import org.fluentjdbc.DbContextTable;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.fluentjdbc.AbstractDatabaseTest.createTable;
import static org.fluentjdbc.AbstractDatabaseTest.dropTableIfExists;

public abstract class DatabaseTableWithArraysTest {

    @Rule
    public final DbContextRule dbContext;

    private final DataSource dataSource;
    private final Map<String, String> replacements;
    private DbContextTable arrayTable;

    public DatabaseTableWithArraysTest(DataSource dataSource, Map<String, String> replacements) {
        this.dataSource = dataSource;
        this.replacements = replacements;
        dbContext = new DbContextRule(dataSource);
        arrayTable = dbContext.table("dbtest_array_table");
    }

    @Before
    public void setupDatabase() {
        dropTableIfExists(dbContext.getThreadConnection(), "dbtest_array_table");
        createTable(dbContext.getThreadConnection(), "create table dbtest_array_table (id ${UUID} primary key, numbers integer[], strings varchar[])", replacements);
    }

    @Test
    public void shouldSaveIntArrayValues() {
        UUID id = arrayTable.newSaveBuilderWithUUID("id", null)
                .setField("numbers", Arrays.asList(1, 2, 3))
                .execute().getId();
        assertThat(arrayTable.where("id", id).singleObject(row -> row.getIntList("numbers")))
                .get()
                .isEqualTo(Arrays.asList(1, 2, 3));
    }

    @Test
    public void shouldRetrieveNullValues() {
        UUID id = arrayTable.newSaveBuilderWithUUID("id", null).execute().getId();
        assertThat(arrayTable.where("id", id).list(row -> row.getIntList("numbers")))
                .isEqualTo(Arrays.asList(new Integer[] { null }));
        assertThat(arrayTable.where("id", id).list(row -> row.getIntList("strings")))
                .isEqualTo(Arrays.asList(new String[] { null }));
    }

    @Test
    public void shouldSaveStringArrayValues() {
        UUID id = arrayTable.newSaveBuilderWithUUID("id", null)
                .setField("strings", Arrays.asList("A", "B", "C"))
                .execute().getId();
        assertThat(arrayTable.where("id", id).singleObject(row -> row.getStringList("strings")))
                .get()
                .isEqualTo(Arrays.asList("A", "B", "C"));
    }

}
