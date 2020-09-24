package org.fluentjdbc.usage.context;

import org.fluentjdbc.DbContextTable;
import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.fluentjdbc.AbstractDatabaseTest.createTable;
import static org.fluentjdbc.AbstractDatabaseTest.dropTableIfExists;

public class DatabaseTableWithArraysTest {

    @Rule
    public final DbContextRule dbContext;

    private final Map<String, String> replacements;
    private DbContextTable arrayTable;
    private boolean supportsTypedArrays = false;

    public DatabaseTableWithArraysTest() {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected DatabaseTableWithArraysTest(DataSource dataSource, Map<String, String> replacements) {
        this.replacements = replacements;
        dbContext = new DbContextRule(dataSource);
        arrayTable = dbContext.table("dbtest_array_table");
    }

    public void supportsTypedArrays() {
        this.supportsTypedArrays = true;
    }

    @Before
    public void setupDatabase() {
        dropTableIfExists(dbContext.getThreadConnection(), "dbtest_array_table");
        createTable(dbContext.getThreadConnection(), "create table dbtest_array_table (id ${UUID} primary key, numbers ${INT_ARRAY}, strings ${STRING_ARRAY})", replacements);
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
        UUID id = arrayTable.newSaveBuilderWithUUID("id", null)
                .setField("numbers", null)
                .setField("strings", null)
                .execute().getId();
        assertThat(arrayTable.where("id", id).list(row -> row.getIntList("numbers")))
                .isEqualTo(Arrays.asList(new Integer[] { null }));
        assertThat(arrayTable.where("id", id).list(row -> row.getIntList("strings")))
                .isEqualTo(Arrays.asList(new String[] { null }));
    }

    @Test
    public void shouldRetrieveEmptyValues() {
        Assume.assumeFalse("Database vendor requires typed arrays so fluent-jdbc doesn't know how to save empty arrays",
                supportsTypedArrays);
        UUID id = arrayTable.newSaveBuilderWithUUID("id", null)
                .setField("numbers", new ArrayList<>())
                .setField("strings", new ArrayList<>())
                .execute().getId();
        assertThat(arrayTable.where("id", id).list(row -> row.getIntList("numbers")))
                .containsExactly(new ArrayList<>());
        assertThat(arrayTable.where("id", id).list(row -> row.getIntList("strings")))
                .containsExactly(new ArrayList<>());
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

    @Test
    public void shouldGiveErrorWhenSavingWrongArrayType() {
        Assume.assumeTrue("Database vendor does not support Typed arrays",
                supportsTypedArrays);
        assertThatThrownBy(() -> arrayTable.newSaveBuilderWithUUID("id", null)
                .setField("numbers", Arrays.asList("A", "B", "C"))
                .execute())
        .isInstanceOf(Exception.class);
    }

    @Test
    public void shouldGiveErrorWhenReadingWrongArrayType() {
        UUID id = arrayTable.newSaveBuilderWithUUID("id", null)
                .setField("strings", Arrays.asList("A", "B", "C"))
                .execute().getId();
        assertThatThrownBy(() -> arrayTable.where("id", id).singleObject(row -> row.getIntList("strings")))
            .isInstanceOf(ClassCastException.class)
            .hasMessageContaining("java.lang.String");
    }

}
