package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    protected ResultSet resultSet;
    private Map<String, DatabaseRow> tableRows = new HashMap<>();

    public DatabaseResult(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
    }

    public boolean next() throws SQLException {
        return resultSet.next();
    }

    // TODO: This doesn't work on Android or SQL server
    public DatabaseRow table(String tableName) throws SQLException {
        if (!tableRows.containsKey(tableName)) {
            tableRows.put(tableName, new DatabaseRow(resultSet, tableName));
        }
        return tableRows.get(tableName);
    }

    public <T> List<T> list(RowMapper<T> mapper) throws SQLException {
        List<T> result = new ArrayList<>();
        while (next()) {
            result.add(mapper.mapRow(createDatabaseRow()));
        }
        return result;
    }

    public void forEach(DatabaseTable.RowConsumer consumer) throws SQLException {
        while (next()) {
            consumer.apply(createDatabaseRow());
        }
    }

    @Nonnull
    public <T> Optional<T> single(RowMapper<T> mapper) throws SQLException {
        if (!next()) {
            return Optional.empty();
        }
        T result = mapper.mapRow(createDatabaseRow());
        if (next()) {
            throw new IllegalStateException("More than one row returned");
        }
        return Optional.of(result);
    }

    protected DatabaseRow createDatabaseRow() throws SQLException {
        return new DatabaseRow(this.resultSet);
    }

    public <T> Stream<T> stream(RowMapper<T> mapper, String query, PreparedStatement stmt) throws SQLException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(mapper, query, stmt), 0), false);
    }

    public <T> Iterator<T> iterator(RowMapper<T> mapper, String query, PreparedStatement stmt) throws SQLException {
        return new Iterator<>(mapper, query, stmt);
    }

    class Iterator<T> implements java.util.Iterator<T> {
        private final RowMapper<T> mapper;
        private final long startTime;
        private final String query;
        private final PreparedStatement stmt;
        private boolean hasNext;

        public Iterator(RowMapper<T> mapper, String query, PreparedStatement stmt) throws SQLException {
            this.mapper = mapper;
            this.startTime = System.currentTimeMillis();
            this.query = query;
            this.stmt = stmt;
            hasNext = resultSet.next();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public T next() {
            try {
                T o = mapper.mapRow(createDatabaseRow());
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
            stmt.close();
        }

        @SuppressWarnings("deprecation")
        @Override
        protected void finalize() throws Throwable {
            close();
        }
    }}
