package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.fluentjdbc.demo.TagType;
import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BulkInsertTest {

    private Connection connection;

    private DatabaseTable demoTable = new DatabaseTableWithTimestamps("bulk_insert_table");


    public BulkInsertTest() throws SQLException {
        this(H2TestDatabase.createConnection());
    }

    protected BulkInsertTest(Connection connection) {
        this.connection = connection;
    }

    @Before
    public void createTables() throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table if exists tag_types");
            stmt.executeUpdate("drop table if exists " + demoTable.getTableName());

            stmt.executeUpdate(preprocessCreateTable(connection, TagType.CREATE_TABLE));
            stmt.executeUpdate("create table bulk_insert_table (id integer primary key auto_increment, type varchar not null, code integer not null, name varchar not null, updated_at datetime not null, created_at datetime not null)");
        }
    }

    // TODO: Move to H2TestDatabase and rename H2TestDatabase
    private static String preprocessCreateTable(Connection connection, String createTableStatement) throws SQLException {
        String productName = connection.getMetaData().getDatabaseProductName();
        if (productName.equals("SQLite")) {
            return createTableStatement.replaceAll("auto_increment", "autoincrement");
        } else {
            return createTableStatement;
        }
    }

    @Test
    public void shouldInsertComplexType() {
        List<Object[]> objects = new ArrayList<>();
        objects.add(new Object[] { "first name", "1" });
        objects.add(new Object[] { "second name", "2" });

        demoTable.newBulkInserter(objects, "type", "code", "name")
            .insert(new InsertMapper<Object[]>() {
                @Override
                public void mapRow(Inserter inserter, Object[] o) throws SQLException {
                    inserter.setField("type", "a");
                    inserter.setField("name", o[0]);
                    inserter.setField("code", o[1]);
                }
            })
            .execute(connection);

        assertThat(demoTable.where("type", "a").listStrings(connection, "name")).contains("first name", "second name");
    }


    @Test
    public void shouldBulkInsert() {
        List<TagType> tagTypes = Arrays.asList(new TagType("a"), new TagType("b"), new TagType("c"));

        TagType.saveAll(tagTypes, connection);

        assertThat(TagType.list(connection))
            .extracting("name")
            .contains("a", "b", "c");
    }


}
