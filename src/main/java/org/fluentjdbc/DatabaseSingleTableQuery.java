package org.fluentjdbc;

public interface DatabaseSingleTableQuery<T extends DatabaseSingleTableQuery<T>> extends DatabaseQueryable<T> {
    DatabaseListableQueryBuilder unordered();

    DatabaseListableQueryBuilder orderBy(String orderByClause);

    DatabaseUpdateBuilder update();

    DatabaseDeleteBuilder delete();
}
