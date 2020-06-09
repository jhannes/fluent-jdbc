package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@ParametersAreNonnullByDefault
public class DatabaseResult implements AutoCloseable {

    private final static Logger logger = LoggerFactory.getLogger(DatabaseResult.class);

    public interface DatabaseResultMapper<T> {

        T apply(DatabaseResult result) throws SQLException;
    }

    private final PreparedStatement statement;
    protected ResultSet resultSet;
    protected final Map<String, Integer> columnIndexes;
    protected final Map<String, Map<String, Integer>> tableColumnIndexes;
    private Map<DatabaseTableAlias, Integer> keys;

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
                tableColumnIndexes.computeIfAbsent(tableName, name -> new HashMap<>()).put(columnName, i);
            }

            if (!columnIndexes.containsKey(columnName)) {
                columnIndexes.put(columnName, i);
            } else {
                logger.warn("Duplicate column " + columnName + " in query result");
            }
        }
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
    }

    public boolean next() throws SQLException {
        return resultSet.next();
    }

    public <T> List<T> list(RowMapper<T> mapper) throws SQLException {
        List<T> result = new ArrayList<>();
        while (next()) {
            result.add(mapper.mapRow(row()));
        }
        return result;
    }

    public void forEach(DatabaseTable.RowConsumer consumer) throws SQLException {
        while (next()) {
            consumer.apply(row());
        }
    }

    @Nonnull
    public <T> Optional<T> single(RowMapper<T> mapper) throws SQLException {
        if (!next()) {
            return Optional.empty();
        }
        T result = mapper.mapRow(row());
        if (next()) {
            throw new IllegalStateException("More than one row returned");
        }
        return Optional.of(result);
    }

    public DatabaseRow row() {
        return new DatabaseRow(this.resultSet, this.columnIndexes, this.tableColumnIndexes, this.keys);
    }

    public <T> Stream<T> stream(RowMapper<T> mapper, String query) throws SQLException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(mapper, query), 0), false);
    }

    public <T> Iterator<T> iterator(RowMapper<T> mapper, String query) throws SQLException {
        return new Iterator<>(mapper, query);
    }

    class Iterator<T> implements java.util.Iterator<T> {
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
    }}
