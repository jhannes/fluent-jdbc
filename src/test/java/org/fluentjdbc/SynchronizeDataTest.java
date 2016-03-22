package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.fluentjdbc.demo.Entry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SynchronizeDataTest {

    private Connection clientConnection;
    private Connection serverConnection;

    @Before
    public void openConnection() throws SQLException {
        JdbcDataSource serverDataSource = new JdbcDataSource();
        serverDataSource.setUrl("jdbc:h2:mem:" + getClass().getName() + "-server");
        serverConnection = serverDataSource.getConnection();

        try(Statement stmt = serverConnection.createStatement()) {
            stmt.executeUpdate(Entry.CREATE_TABLE);
        }


        JdbcDataSource clientDataSource = new JdbcDataSource();
        clientDataSource.setUrl("jdbc:h2:mem:" + getClass().getName());
        clientConnection = clientDataSource.getConnection();

        try(Statement stmt = clientConnection.createStatement()) {
            stmt.executeUpdate(Entry.CREATE_TABLE);
        }
    }

    @After
    public void closeConnections() throws SQLException {
        clientConnection.close();
        serverConnection.close();
    }

    @Test
    public void shouldDownloadChanges() throws Exception {
        Entry entry = new Entry("some name");
        entry.save(serverConnection, new ArrayList<>());


        assertThat(Entry.retrieve(clientConnection, entry.getId())).isNull();

        synchronize(serverConnection, clientConnection);

        assertThat(Entry.retrieve(clientConnection, entry.getId()))
            .isEqualToComparingFieldByField(entry);
    }

    private void synchronize(Connection serverConnection, Connection clientConnection) {
        List<Entry> entries = Entry.list(serverConnection);

        for (Entry entry : entries) {
            entry.save(clientConnection, new ArrayList<>());
        }
    }

}
