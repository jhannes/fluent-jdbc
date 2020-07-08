package org.fluentjdbc;

public interface DbContextConnection extends AutoCloseable {
    @Override
    void close();
}
