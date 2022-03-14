package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

/**
 * An observer used to monitor the timing of queries. All query operations
 * all SELECTs, UPDATEs, INSERTs and DELETEs will execute
 * <code>reporter.table("TABLE").operation("SELECT").timing("SELECT * FROM ...", millis)</code>
 * upon completion. Create implementations of this interface to keep timing information.
 *
 * <p>For example, with DropWizard Metrics you may want to do something like this:</p>
 *
 * <pre>
 * MetricRegistry registry = new MetricRegistry();
 * DatabaseReporter reporter = tableName -&gt; operation -&gt; (query, timing) -&gt; {
 *      registry.timer(tableName + "/" + operation).update(Duration.ofMillis(timing));
 *      registry.timer(tableName + "/" + operation + "/" + query).update(Duration.ofMillis(timing));
 * }
 * DbContext context = new DbContext(new DatabaseStatementFactory(reporter));
 * </pre>
 *
 */
@FunctionalInterface
public interface DatabaseReporter {
    /**
     * Returns a reporter which logs the queries to SLF4J
     */
    DatabaseReporter LOGGING_REPORTER = name -> DatabaseTableReporter.LOGGING_REPORTER;

    /** Used to return a reporter for the specified table */
    @CheckReturnValue
    @Nonnull
    DatabaseTableReporter table(@Nonnull String tableName);
}
