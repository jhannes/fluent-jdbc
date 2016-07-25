package org.fluentjdbc;

import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Pattern;

public class AbstractDatabaseTest {

    private final Map<String, String> replacements;

    public AbstractDatabaseTest(Map<String,String> replacements) {
        this.replacements = replacements;
    }

    protected String preprocessCreateTable(String createTableStatement) throws SQLException {
        return createTableStatement
                .replaceAll(Pattern.quote("${INTEGER_PK}"), replacements.get("INTEGER_PK"))
                .replaceAll(Pattern.quote("${DATETIME}"), replacements.get("DATETIME"))
                ;
    }

}
