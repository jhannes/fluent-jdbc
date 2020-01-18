package org.fluentjdbc;

import javax.annotation.Nullable;
import java.util.Collection;

public interface DatabaseUpdateable<T extends DatabaseUpdateable<T>> {
    T setFields(Collection<String> fields, Collection<?> values);

    T setField(String field, @Nullable Object value);

    default T setFieldIfPresent(String field, @Nullable Object value) {
        if (value != null) {
            setField(field, value);
        }
        //noinspection unchecked
        return (T)this;
    }
}
