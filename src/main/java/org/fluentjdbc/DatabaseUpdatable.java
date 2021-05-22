package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Interface to enforce consistent behavior of <code>INSERT</code> and <code>UPDATE</code> builders
 */
public interface DatabaseUpdatable<T extends DatabaseUpdatable<T>> {
    /**
     * Adds the fieldName to the SQL statement and the value to the parameter list
     */
    @CheckReturnValue
    T setField(String field, @Nullable Object value);

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @CheckReturnValue
    T setFields(Collection<String> fields, Collection<?> values);

    /**
     * Adds the fieldName to the SQL statement and the value to the parameter list, unless value is null
     */
    @CheckReturnValue
    default T setFieldIfPresent(String field, @Nullable Object value) {
        if (value != null) {
            //noinspection ResultOfMethodCallIgnored
            setField(field, value);
        }
        //noinspection unchecked
        return (T)this;
    }
}
