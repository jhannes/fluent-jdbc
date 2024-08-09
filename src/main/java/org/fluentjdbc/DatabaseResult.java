package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Collects together a {@link PreparedStatement} and the resulting {@link ResultSet},
 * as well as metaData to map column names to collect indexes from {@link ResultSetMetaData}.
 * Use {@link #list(RowMapper)}, {@link #forEach(RowConsumer)} and {@link #single}
 * to process the {@link ResultSet}
 */
@ParametersAreNonnullByDefault
public class DatabaseResult implements AutoCloseable {

    private final static Logger logger = LoggerFactory.getLogger(DatabaseResult.class);

    /**
     * Used to execute statements on the whole DatabaseResult. Like
     * {@link java.util.function.Function}, but allows {@link SQLException} to be
     * thrown from {@link #apply(DatabaseResult)}
     */
    @FunctionalInterface
    public interface DatabaseResultMapper<T> {

        @CheckReturnValue
        T apply(DatabaseResult result) throws SQLException;
    }

    /**
     * Functional interface for {@link #single(RowMapper, Supplier, Supplier)} and {@link #list(RowMapper)}.
     * Like {@link java.util.function.Function}, but allows {@link SQLException} to be
     * thrown from {@link #mapRow(DatabaseRow)}
     */
    @FunctionalInterface
    public interface RowMapper<T> {

        @CheckReturnValue
        T mapRow(DatabaseRow row) throws SQLException;
    }

    /**
     * Functional interface for {@link #forEach}. Like {@link java.util.function.Consumer},
     * but allows {@link SQLException} to be thrown from {@link #apply}
     */
    @FunctionalInterface
    public interface RowConsumer {
        void apply(DatabaseRow row) throws SQLException;
    }

    private final PreparedStatement statement;
    protected final ResultSet resultSet;
    protected final Map<String, Integer> columnIndexes;
    protected final Map<String, Map<String, Integer>> tableColumnIndexes;
    private final Map<DatabaseTableAlias, Integer> keys;

    DatabaseResult(PreparedStatement statement, ResultSet resultSet, Map<String, Integer> columnIndexes, Map<String, Map<String, Integer>> aliasColumnIndexes, Map<DatabaseTableAlias, Integer> keys) {
        this.statement = statement;
        this.resultSet = resultSet;
        this.columnIndexes = columnIndexes;
        this.tableColumnIndexes = aliasColumnIndexes;
        this.keys = keys;
    }

    public DatabaseResult(PreparedStatement statement, ResultSet resultSet) throws SQLException {
        this(statement, resultSet, new HashMap<>(), new HashMap<>(), new HashMap<>());
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i).toUpperCase();
            String tableName = metaData.getTableName(i).toUpperCase();
            if (!tableName.equals("")) {
                if (!tableColumnIndexes.containsKey(tableName)) {
                    tableColumnIndexes.put(tableName, new HashMap<>());
                }
                if (tableColumnIndexes.get(tableName).containsKey(columnName)) {
                    logger.warn("Duplicate column {}.{} in query result", tableName, columnName);
                } else {
                    tableColumnIndexes.get(tableName).put(columnName, i);
                }
            }

            if (!columnIndexes.containsKey(columnName)) {
                columnIndexes.put(columnName, i);
            } else {
                logger.debug("Duplicate column {} in query result", columnName);
            }
        }
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
    }

    /**
     * Position the underlying {@link ResultSet} on the next row
     */
    @CheckReturnValue
    public boolean next() throws SQLException {
        return resultSet.next();
    }

    /**
     * Call mapper for each row in the {@link ResultSet} and return the result as
     * a List. If {@link RowMapper} throws {@link SQLException}, processing is aborted
     * and the exception is rethrown
     *
     * @param mapper Function to be called for each row
     */
    @CheckReturnValue
    public <T> List<T> list(RowMapper<T> mapper) throws SQLException {
        List<T> result = new ArrayList<>();
        while (next()) {
            result.add(mapper.mapRow(row()));
        }
        return result;
    }

    /**
     * Returns a {@link Stream} which iterates over all rows in the {@link ResultSet} and apply a
     * {@link RowMapper} to each.
     * 
     * @see DatabaseTableQueryBuilder#stream(Connection, RowMapper)
     *
     * @param mapper Function to be called for each row
     * @param query The SQL that was used to generate this {@link DatabaseResult}. Used for logging
     */
    @CheckReturnValue
    public <T> Stream<T> stream(RowMapper<T> mapper, String query) throws SQLException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(mapper, query), 0), false);
    }

    /**
     * Returns an {@link Iterator} which iterates over all rows in the {@link ResultSet} and apply a
     * {@link RowMapper} to each.
     *
     * @param mapper Function to be called for each row
     * @param query The SQL that was used to generate this {@link DatabaseResult}. Used for logging
     */
    @CheckReturnValue
    public <T> Iterator<T> iterator(RowMapper<T> mapper, String query) throws SQLException {
        return new Iterator<>(mapper, query);
    }

    /**
     * Call {@link RowConsumer} for each row in the {@link ResultSet}.
     * If {@link RowMapper} throws {@link SQLException}, processing is aborted and the exception is
     * rethrown
     */
    public void forEach(RowConsumer consumer) throws SQLException {
        while (next()) {
            consumer.apply(row());
        }
    }

    /**
     * Call {@link RowConsumer} for the first row in the {@link ResultSet}. Should only be used
     * where the query parameter ensure that no more than one row can be returned, ie the query
     * should include a unique key.
     *
     * @return the mapped row. If no rows were returned, returns {@link SingleRow#absent}
     * @throws MultipleRowsReturnedException if more than one row was returned
     * @throws SQLException if the {@link RowMapper} throws
     */
    @Nonnull
    @CheckReturnValue
    <T> SingleRow<T> single(RowMapper<T> mapper, Supplier<RuntimeException> noMatchException, Supplier<MultipleRowsReturnedException> multipleRowsException) throws SQLException {
        if (!next()) {
            return SingleRow.absent(noMatchException);
        }
        T result = mapper.mapRow(row());
        if (next()) {
            throw multipleRowsException.get();
        }
        return SingleRow.of(result);
    }

    /**
     * Returns a {@link DatabaseRow} for the current row, allowing mapping retrieval and conversion
     * of data in all columns
     */
    @CheckReturnValue
    public DatabaseRow row() {
        return new DatabaseRow(this.resultSet, this.columnIndexes, this.tableColumnIndexes, this.keys);
    }

    private class Iterator<T> implements java.util.Iterator<T> {
        private final RowMapper<T> mapper;
        private final long startTime;
        private final String query;
        private boolean hasNext;

        public Iterator(RowMapper<T> mapper, String query) throws SQLException {
            this.mapper = mapper;
            this.startTime = System.currentTimeMillis();
            this.query = query;
            hasNext = resultSet.next();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public T next() {
            try {
                T o = mapper.mapRow(row());
                hasNext = resultSet.next();
                if (!hasNext) {
                    logger.debug("time={}s query=\"{}\"", (System.currentTimeMillis()- startTime)/1000.0, query);
                    close();
                }
                return o;
            } catch (SQLException e) {
                throw ExceptionUtil.softenCheckedException(e);
            }
        }

        protected void close() throws SQLException {
            resultSet.close();
            statement.close();
        }

        @SuppressWarnings("deprecation")
        @Override
        protected void finalize() throws Throwable {
            close();
        }
    }
}
