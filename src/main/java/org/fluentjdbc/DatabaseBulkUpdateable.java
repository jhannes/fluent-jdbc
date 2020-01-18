package org.fluentjdbc;

import java.util.List;
import java.util.function.Function;

public interface DatabaseBulkUpdateable<ENTITY, SELF extends DatabaseBulkUpdateable<ENTITY, SELF>> {
    SELF setField(String fieldName, Function<ENTITY, Object> transformer);

    default <VALUES extends List<?>> SELF setFields(List<String> fields, Function<ENTITY, VALUES> values) {
        for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
            int index = i;
            setField(fields.get(i), o -> values.apply(o).get(index));
        }
        //noinspection unchecked
        return (SELF) this;
    }


}
