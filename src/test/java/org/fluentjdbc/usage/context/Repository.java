package org.fluentjdbc.usage.context;

import org.fluentjdbc.DatabaseSaveResult;

import java.util.Optional;
import java.util.stream.Stream;

public interface Repository<T, ID> {
    DatabaseSaveResult.SaveStatus save(T product);

    Repository.Query<T> query();

    Optional<T> retrieve(ID id);

    interface Query<T> {
        Stream<T> stream();
    }
}
