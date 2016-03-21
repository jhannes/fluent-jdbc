package org.fluentjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabaseStatement {
    protected static Logger logger = LoggerFactory.getLogger(DatabaseStatement.class);

    protected List<Object> parameters = new ArrayList<>();

    protected int bindParameters(PreparedStatement stmt) throws SQLException {
        int index = 1;
        for (Object parameter : parameters) {
            bindParameter(stmt, index++, parameter);
        }
        return index;
    }

    protected void bindParameter(PreparedStatement stmt, int index, Object parameter) throws SQLException {
        stmt.setObject(index, parameter);
    }

}
