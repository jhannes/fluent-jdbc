package org.fluentjdbc;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@ParametersAreNonnullByDefault
public class DatabaseWhereBuilder implements DatabaseQueryable<DatabaseWhereBuilder> {

    private final List<String> conditions = new ArrayList<>();
    private final List<Object> parameters = new ArrayList<>();

    @Override
    public DatabaseWhereBuilder whereExpressionWithParameterList(String expression, Collection<?> parameters) {
        this.conditions.add(expression);
        this.parameters.addAll(parameters);
        return this;
    }

    @Override
    public DatabaseWhereBuilder query() {
        return this;
    }

    public String whereClause() {
        return conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    }

    public List<Object> getParameters() {
        return parameters;
    }
}
