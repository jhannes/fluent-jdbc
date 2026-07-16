package org.fluentjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface DatabaseTransactionReporter {
    Logger logger = LoggerFactory.getLogger(DatabaseTransactionReporter.class);

    DatabaseTransactionReporter LOGGING_REPORTER = (commit, timing) -> logger.debug("time={}s " + (commit ? "commit" : "rollback"), timing/1000.0);

    default void logRollback(long timing) {
        doLog(false, timing);
    }

    default void logCommit(long timing) {
        doLog(true, timing);
    }

    void doLog(boolean commit, long timing);
}
