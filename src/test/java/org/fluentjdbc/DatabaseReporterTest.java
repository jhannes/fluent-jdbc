package org.fluentjdbc;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.fluentjdbc.AbstractDatabaseTest.createTable;
import static org.fluentjdbc.AbstractDatabaseTest.dropTableIfExists;

public class DatabaseReporterTest {

    @Rule
    public final DbContextRule dbContext;

    private final DataSource dataSource;

    private final DbContextTable table;

    private final Map<String, String> replacements;
    private final MetricRegistry metricRegistry = new MetricRegistry();

    public DatabaseReporterTest() {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected DatabaseReporterTest(DataSource dataSource, Map<String, String> replacements) {
        this.dbContext = new DbContextRule(dataSource, new DatabaseStatementFactory(tableName -> operation -> {
            Timer counter = metricRegistry.timer(tableName + "/" + operation);
            return (query, duration) -> counter.update(Duration.ofMillis(duration));
        }));
        this.dataSource = dataSource;
        this.table = dbContext.table("unique_table_name");
        this.replacements = replacements;
    }

    @Before
    public void setupDatabase() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            dropTableIfExists(connection, "unique_table_name");
            createTable(connection, "create table unique_table_name (id ${INTEGER_PK}, code integer not null, name varchar(50) null)", replacements);
        }
    }

    @Test
    public void shouldCountSelectCounts() {
        Timer histogram = metricRegistry.timer("unique_table_name/COUNT");
        long countBefore = histogram.getCount();
        int result = table.query().getCount();
        assertThat(result).isZero();
        assertThat(histogram.getCount()).isEqualTo(countBefore + 1);
    }

    @Test
    public void shouldCountSelects() {
        Timer histogram = metricRegistry.timer("unique_table_name/SELECT");
        long countBefore = histogram.getCount();
        assertThat(table.query().listStrings("name")).isEmpty();
        assertThat(histogram.getCount()).isEqualTo(countBefore + 1);
    }

}
