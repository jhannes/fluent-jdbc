package org.fluentjdbc;

import java.sql.SQLException;

import org.fluentjdbc.util.ExceptionUtil;

public class DbInsertContext {

    public class DbInsertContextWithPk<T> {

        private DatabaseInsertWithPkBuilder<T> builder2;

        public DbInsertContextWithPk(DatabaseInsertWithPkBuilder<T> builder) {
            builder2 = builder;
        }

        public DbInsertContextWithPk<T> setField(String fieldName, Object parameter) {
            builder2.setField(fieldName, parameter);
            return this;
        }

        public T execute() {
            try {
                return builder2.execute(dbTableContext.getConnection());
            } catch (SQLException e) {
                throw ExceptionUtil.softenCheckedException(e);
            }
        }
    }

    private DatabaseInsertBuilder builder;
    private DbTableContext dbTableContext;

    public DbInsertContext(DbTableContext dbTableContext) {
        this.dbTableContext = dbTableContext;
        builder = new DatabaseInsertBuilder(dbTableContext.getTable().getTableName());
    }

    public <T> DbInsertContextWithPk<T> setPrimaryKey(String idField, T idValue) {
        DatabaseInsertWithPkBuilder<T> setPrimaryKey = builder.setPrimaryKey(idField, idValue);
        return new DbInsertContextWithPk<T>(setPrimaryKey);
    }

    public DbInsertContext setField(String fieldName, Object parameter) {
        builder.setField(fieldName, parameter);
        return this;
    }

    public void execute() {
        builder.execute(dbTableContext.getConnection());
    }
}
