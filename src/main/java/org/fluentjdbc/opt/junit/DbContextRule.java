package org.fluentjdbc.opt.junit;

import org.fluentjdbc.DbContext;
import org.fluentjdbc.DbContextConnection;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.sql.DataSource;

public class DbContextRule extends DbContext implements TestRule {
    private final DataSource dataSource;

    public DbContextRule(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try (DbContextConnection ignored = startConnection(dataSource)) {
                    statement.evaluate();
                }
            }
        };
    }
}
