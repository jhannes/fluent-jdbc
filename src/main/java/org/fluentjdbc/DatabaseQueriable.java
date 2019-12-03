package org.fluentjdbc;

import javax.annotation.Nullable;
import java.util.Collection;

public interface DatabaseQueriable<T extends DatabaseQueriable<T>> {

    T whereExpressionWithMultipleParameters(String expression, Collection<?> parameters);

    T whereExpression(String expression, @Nullable Object value);

    T whereExpression(String expression);

    T whereOptional(String fieldName, @Nullable Object value);

    T whereIn(String fieldName, Collection<?> parameters);

    default T where(String fieldName, @Nullable Object value) {
        return whereExpression(fieldName + " = ?", value);
    }
}
