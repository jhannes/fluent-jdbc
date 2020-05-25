package org.fluentjdbc;

import java.util.Optional;

public interface RetrieveMethod<KEY, ENTITY> {
    Optional<ENTITY> retrieve(KEY key);
}
