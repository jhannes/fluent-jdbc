package org.fluentjdbc;

/**
 * Specified a context where a {@link java.sql.Connection} is associated with the current thread
 * in order to perform operations on {@link DbTableContext}. Example:
 *
 * <pre>
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *     return table.where("id", id).orderBy("data").listLongs("data");
 * }
 * </pre>
 */
public interface DbContextConnection extends AutoCloseable {
    @Override
    void close();
}
