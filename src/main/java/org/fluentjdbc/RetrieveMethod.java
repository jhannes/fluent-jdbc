package org.fluentjdbc;

public interface RetrieveMethod<KEY, ENTITY> {
    ENTITY retrieve(KEY key);
}
