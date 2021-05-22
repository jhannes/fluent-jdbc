package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.function.Function;

/**
 * Fluently create a statement for a list of objects with a <code>WHERE ...</code> clause,
 * such as <code>UPDATE </code> or <code>DELETE</code>
 */
public interface DatabaseBulkQueryable<ENTITY, SELF extends DatabaseBulkQueryable<ENTITY, SELF>> {
    /**
     * Adds a function that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk update
     * to extract the values for the <code>SET fieldName = ?</code> clause
     */
    @CheckReturnValue
    SELF where(String field, Function<ENTITY, ?> value);

    /**
     * Adds a list of functions that will be called for each object to get the value for
     * {@link PreparedStatement#setObject(int, Object)} for each row in the bulk update
     * to extract the values for the <code>SET fieldName1 = ?, fieldName2 = ?, ...</code> clause
     */
    @CheckReturnValue
    default SELF whereAll(List<String> fields, Function<ENTITY, List<?>> values) {
        for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
            int index = i;
            //noinspection ResultOfMethodCallIgnored
            where(fields.get(i), o -> values.apply(o).get(index));
        }
        //noinspection unchecked
        return (SELF) this;
    }
}
