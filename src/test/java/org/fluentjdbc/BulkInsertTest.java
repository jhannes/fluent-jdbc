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
import java.util.Map;

public class BulkInsertTest extends AbstractDatabaseTest {

    private Connection connection;

    private DatabaseTable demoTable = new DatabaseTableWithTimestamps("bulk_insert_table");


    public BulkInsertTest() throws SQLException {
        this(H2TestDatabase.createConnection(), H2TestDatabase.REPLACEMENTS);
    }

    protected BulkInsertTest(Connection connection, Map<String, String> replacements) {
        super(replacements);
        this.connection = connection;
    }

    @Before
    public void createTables() throws SQLException {
        dropTableIfExists(connection, demoTable.getTableName());
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(preprocessCreateTable("create table bulk_insert_table (id ${INTEGER_PK}, type varchar(50) not null, code integer not null, name varchar(50) not null, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)"));
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
