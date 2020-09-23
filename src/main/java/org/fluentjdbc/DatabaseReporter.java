package org.fluentjdbc;

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
 * DatabaseReporter reporter = tableName -> operation -> (query, timing) -> {
 *      registry.timer(tableName + "/" + operation).update(Duration.ofMilling(timing);
 *      registry.timer(tableName + "/" + operation + "/" + query).update(Duration.ofMilling(timing);
 * }
 * DbContext context = new DbContext(reporter);
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
    DatabaseTableReporter table(String tableName);
}
