package org.fluentjdbc;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.function.Function;

/**
 * Fluently create a statement for a list of objects which updates values, ie <code>INSERT</code>
 * or <code>UPDATE</code>
 */
public interface DatabaseBulkUpdatable<ENTITY, SELF extends DatabaseBulkUpdatable<ENTITY, SELF>> {

    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} parameter for each row in the bulk update
     * to extract the values for fieldName parameter
     */
    SELF setField(String fieldName, Function<ENTITY, Object> transformer);

    /**
     * Adds a list function that will be called for each object to get the value
     * each of the corresponding parameters in the {@link PreparedStatement}
     */
    default <VALUES extends List<?>> SELF setFields(List<String> fields, Function<ENTITY, VALUES> values) {
        for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
            int index = i;
            setField(fields.get(i), o -> values.apply(o).get(index));
        }
        //noinspection unchecked
        return (SELF) this;
    }


}
