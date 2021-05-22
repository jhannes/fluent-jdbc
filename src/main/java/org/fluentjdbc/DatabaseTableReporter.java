package org.fluentjdbc;

import javax.annotation.CheckReturnValue;

/**
 * Observer class used to monitor the timing of queries.
 *
 * @see DatabaseReporter
 */
@FunctionalInterface
public interface DatabaseTableReporter {
    DatabaseTableReporter LOGGING_REPORTER = operation -> DatabaseTableOperationReporter.LOGGING_OPERATION_REPORTER;

    /** Used to return a reporter for an operation on the table */
    @CheckReturnValue
    DatabaseTableOperationReporter operation(String operation);
}
