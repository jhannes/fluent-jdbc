package org.fluentjdbc;

public interface DbTransaction extends AutoCloseable {
    @Override
    void close();

    void setComplete();

    void setRollback();
}
