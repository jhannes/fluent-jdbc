package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class FluentJdbcDemonstrationTest {


    private DatabaseTable databaseTable = new DatabaseTableWithTimestamps("demo_table");

    private Connection connection;

    @Before
    public void openConnection() throws SQLException {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:" + getClass().getName());

        connection = dataSource.getConnection();

        connection.createStatement().executeUpdate("create table demo_table (id integer primary key auto_increment, code integer not null, name varchar not null)");
    }

    @After
    public void closeConnection() throws SQLException {
        connection.close();
    }

    @Test
    public void shouldGenerateIdForNewRow() throws Exception {
        String savedName = "demo row";
        long id = databaseTable
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);

        String retrievedName = databaseTable.where("id", id).singleString(connection, "name");
        assertThat(retrievedName).isEqualTo(savedName);
    }

    @Test
    public void shouldUpdateRowWithExistingId() {
        String savedName = "demo row";
        long id = databaseTable
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);
        String updatedName = "updated name";
        databaseTable
                .newSaveBuilder("id", id)
                .setField("name", updatedName)
                .execute(connection);

        String retrievedName = databaseTable.where("id", id).singleString(connection, "name");
        assertThat(retrievedName).isEqualTo(updatedName);
    }

}
