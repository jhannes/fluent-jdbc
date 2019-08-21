package org.fluentjdbc;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public interface DatabaseQueriable<T extends DatabaseQueriable<T>> {

    T where(String fieldName, @Nullable Object value);

    T whereExpression(String expression, @Nullable Object value);

    T whereExpression(String expression);

    T whereOptional(String fieldName, @Nullable Object value);

    T whereIn(String fieldName, Collection<?> parameters);

}
