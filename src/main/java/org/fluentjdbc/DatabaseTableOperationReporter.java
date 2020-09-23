package org.fluentjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observer interface used to monitor a specific type of operation for a specific table
 *
 * @see DatabaseReporter
 */
@FunctionalInterface
public interface DatabaseTableOperationReporter {
    Logger logger = LoggerFactory.getLogger(DatabaseTableOperationReporter.class);

    DatabaseTableOperationReporter LOGGING_OPERATION_REPORTER =
            (query, timing) -> logger.debug("time={}s query=\"{}\"", timing/1000.0, query);

    /**
     * Called when the operation is performed on the table
     * @param query The parameterized SQL query that was executed to the database
     * @param timing The duration the query took in millis
     */
    void reportQuery(String query, long timing);
}
