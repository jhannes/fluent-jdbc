package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class BulkInsertTest extends AbstractDatabaseTest {

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
            stmt.executeUpdate("drop table if exists " + demoTable.getTableName());
            stmt.executeUpdate(preprocessCreateTable(connection, "create table bulk_insert_table (id integer primary key auto_increment, type varchar not null, code integer not null, name varchar not null, updated_at datetime not null, created_at datetime not null)"));
        }
    }

    @Test
    public void shouldInsertComplexType() {
        List<Object[]> objects = new ArrayList<>();
        objects.add(new Object[] { "first name", 1 });
        objects.add(new Object[] { "second name", 2 });

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




}
