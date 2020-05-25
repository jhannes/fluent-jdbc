package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.h2.H2TestDatabase;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SynchronizeDataTest extends AbstractDatabaseTest {

    public SynchronizeDataTest() {
        super(H2TestDatabase.REPLACEMENTS);
    }

    private static final String CREATE_TABLE =
            "create table demo_table (id ${INTEGER_PK}, name varchar not null, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)";

    private Connection clientConnection;
    private Connection serverConnection;

    private DatabaseTable table = new DatabaseTableWithTimestamps("demo_table");

    @Before
    public void openConnection() throws SQLException {
        JdbcDataSource serverDataSource = new JdbcDataSource();
        serverDataSource.setUrl("jdbc:h2:mem:" + getClass().getName() + "-server");
        serverConnection = serverDataSource.getConnection();

        try(Statement stmt = serverConnection.createStatement()) {
            stmt.executeUpdate(preprocessCreateTable(CREATE_TABLE));
        }


        JdbcDataSource clientDataSource = new JdbcDataSource();
        clientDataSource.setUrl("jdbc:h2:mem:" + getClass().getName());
        clientConnection = clientDataSource.getConnection();

        try(Statement stmt = clientConnection.createStatement()) {
            stmt.executeUpdate(preprocessCreateTable(CREATE_TABLE));
        }
    }

    @After
    public void closeConnections() throws SQLException {
        clientConnection.close();
        serverConnection.close();
    }

    @Test
    public void shouldDownloadChanges() throws Exception {
        Long id = table.newSaveBuilder("id", null).setField("name", "some name").execute(serverConnection).getId();

        assertThat(table.where("id", id).singleString(clientConnection, "name")).isEmpty();

        synchronize(serverConnection, clientConnection);

        assertThat(table.where("id", id).singleString(clientConnection, "name")).get().isEqualTo("some name");
    }

    private void synchronize(Connection serverConnection, Connection clientConnection) throws SQLException {
        List<String> names = table.unordered().list(serverConnection, row -> row.getString("name"));
        for (String name : names) {
            table.newSaveBuilder("id", null).setField("name", name).execute(clientConnection);
        }
    }

}
