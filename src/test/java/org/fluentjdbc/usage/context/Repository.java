package org.fluentjdbc.usage.context;

import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.SingleRow;

import java.util.stream.Stream;

public interface Repository<T, ID> {
    DatabaseSaveResult.SaveStatus save(T row);

    Repository.Query<T> query();

    SingleRow<T> retrieve(ID id);

    interface Query<T> {
        Stream<T> stream();
    }
}
