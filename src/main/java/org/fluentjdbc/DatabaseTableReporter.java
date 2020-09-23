package org.fluentjdbc;

/**
 * Observer class used to monitor the timinig of queries.
 *
 * @see DatabaseReporter
 */
@FunctionalInterface
public interface DatabaseTableReporter {
    DatabaseTableReporter LOGGING_REPORTER = operation -> DatabaseTableOperationReporter.LOGGING_OPERATION_REPORTER;

    /** Used to return a reporter for an operation on the table */
    DatabaseTableOperationReporter operation(String operation);
}
