package org.fluentjdbc;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Interface to enforce consistent behavior of <code>INSERT</code> and <code>UPDATE</code> builders
 */
public interface DatabaseUpdatable<T extends DatabaseUpdatable<T>> {
    /**
     * Adds parameters to the <code>INSERT</code> or <code>UPDATE</code> statement and to the list of parameters
     */
    @CheckReturnValue
    T addParameters(List<DatabaseQueryParameter> queryParameters);

    /**
     * Adds a parameter to the <code>INSERT</code> or <code>UPDATE</code> statement and to the list of parameters
     */
    @CheckReturnValue
    T addParameter(DatabaseQueryParameter parameter);

    /**
     * Adds the fieldName to the SQL statement with the expression and the values to the parameter list
     */
    @CheckReturnValue
    default T setField(String field, String expression, Collection<?> values) {
        return addParameter(new DatabaseQueryParameter(field + " = " + expression, values, field, expression));
    }

    /**
     * Adds the fieldName to the SQL statement and the value to the parameter list
     */
    @CheckReturnValue
    default T setField(String field, @Nullable Object value) {
        return setField(field, "?", Collections.singleton(value));
    }

    /**
     * Calls {@link #setField(String, Object)} for each fieldName and parameter
     */
    @CheckReturnValue
    T setFields(List<String> fields, List<?> values);

    /**
     * Calls {@link #setField(String, Object)} for each key and value in the parameter map
     */
    @CheckReturnValue
    T setFields(Map<String, ?> fields);

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
