package org.fluentjdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class Inserter extends DatabaseStatement {

    private final PreparedStatement statement;
    private final List<String> fields;

    Inserter(PreparedStatement statement, List<String> fields) {
        this.statement = statement;
        this.fields = fields;
    }

    public void setField(String fieldName, Object value) throws SQLException {
        int position = fields.indexOf(fieldName);
        if (position < 0) {
            throw new IllegalArgumentException("Unknown field " + fieldName);
        }
        bindParameter(statement, position+1, value);
    }

}
