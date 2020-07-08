package org.fluentjdbc;

import java.util.List;
import java.util.function.Function;

public interface DatabaseBulkQueryable<ENTITY, SELF extends DatabaseBulkQueryable<ENTITY, SELF>> {
    SELF where(String field, Function<ENTITY, ?> value);

    default SELF whereAll(List<String> fields, Function<ENTITY, List<?>> values) {
        for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
            int index = i;
            where(fields.get(i), o -> values.apply(o).get(index));
        }
        //noinspection unchecked
        return (SELF) this;
    }
}
