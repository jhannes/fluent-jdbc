package org.fluentjdbc;

import javax.annotation.Nullable;
import java.util.List;

public interface DatabaseUpdateable<T extends DatabaseUpdateable<T>> {
    T setFields(List<String> fields, List<Object> values);

    T setField(String field, @Nullable Object value);

    T setFieldIfPresent(String field, @Nullable Object value);
}
