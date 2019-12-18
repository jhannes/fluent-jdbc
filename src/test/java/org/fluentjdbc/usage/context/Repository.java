package org.fluentjdbc.usage.context;

import org.fluentjdbc.DatabaseSaveResult;

import java.util.List;
import java.util.UUID;

public interface Repository<T, ID> {
    DatabaseSaveResult.SaveStatus save(T product);

    Repository.Query<T> query();

    T retrieve(ID id);

    interface Query<T> {
        List<T> list();
    }
}
