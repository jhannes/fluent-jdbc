package org.fluentjdbc;

/**
 * Controls whether the current thread will commit or rollback operations to database. If
 * {@link #setComplete()} is called, the transaction will be committed, unless {@link #setRollback()}
 * is called afterwards. Default behavior is to rollback if nothing else is specified.
 *
 * <p>Transactions can be nested. For example, if one operation rolls back, the whole context
 * is rolled back:</p>
 * <pre>
 * try (DbTransaction tx = dbContext.ensureTransaction()) {
 *     // ... some operations
 *     try (DbTransaction innerTx = dbContext.ensureTransaction()) {
 *         // ... some operations
 *         innerTx.setRollback();
 *     }
 *     tx.setComplete();
 * }
 * // Operations will be rolled back as the inner transaction was rolled back
 * </pre>
 *
 */
public interface DbTransaction extends AutoCloseable {
    @Override
    void close();

    /**
     * Specify that this transaction should be committed when {@link #close()} is called.
     * Subsequent calls to {@link #setRollback()} will reset this behavior. If an inner
     * transaction votes {@link #setRollback()}, the transaction will be rolled back
     */
    void setComplete();

    /**
     * Specify that this transaction and outer transactions will be rolled back. This
     * is the default behavior
     */
    void setRollback();
}
