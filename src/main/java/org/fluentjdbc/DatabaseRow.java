package org.fluentjdbc;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Retrieves column values for a single row returned by a query, if necessary, calculating
 * the column position and doing necessary conversion. {@link DatabaseRow} serves as an
 * encapsulation of a {@link ResultSet} for a single row, with the additional support for
 * converting {@link Instant}s, {@link ZonedDateTime}s {@link OffsetDateTime}s, {@link LocalDate}s,
 * {@link UUID}s, and {@link Enum}s. {@link #getColumnIndex(String)} supports joins for most database
 * drivers by reading column names from {@link java.sql.ResultSetMetaData}. {@link #table(String)}
 * and {@link #table(DatabaseTableAlias)} returns a DatabaseRow where all offsets are relative to the
 * specified table.
 *
 * Example usage:
 * <pre>
 * DatabaseTable organizations = new DatabaseTableImpl("organizations");
 * DatabaseTable persons = new DatabaseTableImpl("persons");
 * DatabaseTable memberships = new DatabaseTableImpl("memberships");
 *
 * DatabaseTableAlias m = memberships.alias("m");
 * DatabaseTableAlias p = persons.alias("p");
 * DatabaseTableAlias o = organizations.alias("o");
 *
 * List&lt;Member&lt; result = m.join(m.column("person_id"), p.column("id"))
 *         .join(m.column("organization_id"), o.column("id"))
 *         .list(connection, row -&gt;s {
 *             Member member = new Member();
 *             member.setName(row.table(p).getString("name"));
 *             member.setBirthDate(row.table(p).getLocalDate("birthdate"));
 *             member.setOrganizationName(row.table(o).getString("name"));
 *             member.setStatus(row.table(m).getEnum(MembershipStatus.class, "name"));
 *             return person;
 *         }
 * </pre>
 */
public class DatabaseRow {

    private final Map<String, Integer> columnIndexes;
    private final Map<String, Map<String, Integer>> tableColumnIndexes;
    private final Map<DatabaseTableAlias, Integer> keys;
    protected final ResultSet rs;

    protected DatabaseRow(ResultSet rs, Map<String, Integer> columnIndexes, Map<String, Map<String, Integer>> tableColumnIndexes, Map<DatabaseTableAlias, Integer> keys) {
        this.rs = rs;
        this.columnIndexes = columnIndexes;
        this.tableColumnIndexes = tableColumnIndexes;
        this.keys = keys;
    }

    /**
     * Returns the underlying database-representation for the specified column
     */
    public Object getObject(String column) throws SQLException {
        return rs.getObject(getColumnIndex(column));
    }

    /**
     * Returns the value of the specified column on this row as a string
     */
    public String getString(String column) throws SQLException {
        return rs.getString(getColumnIndex(column));
    }

    /**
     * Returns the long value of the specified column on this row. If the
     * column value is null, returns null (unlike {@link ResultSet#getLong(int)}
     *
     * @see #getColumnIndex
     */
    public Long getLong(String column) throws SQLException {
        long result = rs.getLong(getColumnIndex(column));
        return rs.wasNull() ? null : result;
    }

    /**
     * Returns the Integer value of the specified column on this row. If the
     * column value is null, returns null (unlike {@link ResultSet#getInt(int)}
     *
     * @see #getColumnIndex
     */
    public Integer getInt(String column) throws SQLException {
        int result = rs.getInt(getColumnIndex(column));
        return rs.wasNull() ? null : result;
    }

    /**
     * Returns the Double value of the specified column on this row. If the
     * column value is null, returns null (unlike {@link ResultSet#getDouble(int)}
     *
     * @see #getColumnIndex
     */
    public Double getDouble(String column) throws SQLException {
        double result = rs.getDouble(getColumnIndex(column));
        return !rs.wasNull() ? result : null;
    }

    /**
     * Returns the value of the specified column on this row as a boolean
     *
     * @see #getColumnIndex
     */
    public boolean getBoolean(String column) throws SQLException {
        return rs.getBoolean(getColumnIndex(column));
    }

    /**
     * Returns the value of the specified column on this row as a timestamp
     *
     * @see #getColumnIndex
     */
    public Timestamp getTimestamp(String column) throws SQLException {
        return rs.getTimestamp(getColumnIndex(column));
    }

    /**
     * Returns the value of the specified column on this row as an Instant
     *
     * @see #getColumnIndex
     */
    public Instant getInstant(String column) throws SQLException {
        Timestamp timestamp = getTimestamp(column);
        return timestamp != null ? timestamp.toInstant() : null;
    }

    /**
     * Returns the value of the specified column on this row as a ZonedDateTime
     *
     * @see #getColumnIndex
     */
    public ZonedDateTime getZonedDateTime(String fieldName) throws SQLException {
        Instant instant = getInstant(fieldName);
        return instant != null ? instant.atZone(ZoneId.systemDefault()) : null;
    }

    /**
     * Returns the value of the specified column on this row as a OffsetDateTime
     *
     * @see #getColumnIndex
     */
    public OffsetDateTime getOffsetDateTime(String fieldName) throws SQLException {
        Instant instant = getInstant(fieldName);
        return instant != null ? OffsetDateTime.ofInstant(instant, ZoneId.systemDefault()) : null;
    }

    /**
     * Returns the value of the specified column on this row as a LocalDate
     *
     * @see #getColumnIndex
     */
    public LocalDate getLocalDate(String column) throws SQLException {
        Date date = rs.getDate(getColumnIndex(column));
        return date != null ? date.toLocalDate() : null;
    }

    /**
     * Returns the value of the specified column on this row as a String converted to {@link UUID}
     *
     * @see #getColumnIndex
     */
    public UUID getUUID(String fieldName) throws SQLException {
        String result = getString(fieldName);
        return result != null ? UUID.fromString(result) : null;
    }

    /**
     * Returns the value of the specified column on this row as a BigDecimal
     *
     * @see #getColumnIndex
     */
    public BigDecimal getBigDecimal(String column) throws SQLException {
        return rs.getBigDecimal(getColumnIndex(column));
    }

    public List<Integer> getIntList(String columnName) throws SQLException {
        Array array = rs.getArray(getColumnIndex(columnName));
        return array != null ? Arrays.asList((Integer[]) array.getArray()) : null;
    }

    public List<String> getStringList(String columnName) throws SQLException {
        Array array = rs.getArray(getColumnIndex(columnName));
        return array != null ? Arrays.asList((String[]) array.getArray()) : null;
    }

    /**
     * Returns the value of the specified column on this row as a Enum of the specified type.
     * Retrieves the column value as String and converts it to the specified enum
     *
     * @see #getColumnIndex
     * @throws IllegalArgumentException if the specified enum type has
     *         no constant with the specified name, or the specified
     *         class object does not represent an enum type
     */
    public <T extends Enum<T>> T getEnum(Class<T> enumClass, String fieldName) throws SQLException {
        String value = getString(fieldName);
        return value != null ? Enum.valueOf(enumClass, value) : null;
    }

    /**
     * Returns the numeric index of the specified column in the current context. If {@link #table}
     * has been called to specify a table or table alias in a join statement, this method can resolve
     * ambiguous column names
     *
     * @return the index to be used with {@link ResultSet#getObject(int)} etc
     * @throws IllegalArgumentException if the fieldName was not present in the ResultSet
     */
    protected Integer getColumnIndex(String fieldName) {
        if (!columnIndexes.containsKey(fieldName.toUpperCase())) {
            throw new IllegalArgumentException("Column {" + fieldName + "} is not present in " + columnIndexes.keySet());
        }
        return columnIndexes.get(fieldName.toUpperCase());
    }

    /**
     * Extracts a {@link DatabaseRow} for the specified {@link DatabaseTableAlias} and maps it over the
     * {@link DatabaseResult.RowMapper} function to return an object mapped from the
     * part of the {@link DatabaseRow} belonging to the specified alias. If the alias was the result
     * of an outer join that didn't return data, this method will instead return {@link Optional#empty()}
     *
     * @see DatabaseJoinedQueryBuilder
     */
    public <T> Optional<T> table(DatabaseTableAlias alias, DatabaseResult.RowMapper<T> function) throws SQLException {
        DatabaseRow table = table(alias);
        return table != null ? Optional.of(function.mapRow(table)) : Optional.empty();
    }

    /**
     * Extracts a {@link DatabaseRow} for the specified {@link DatabaseTableAlias} belonging to the
     * specified alias. If the alias was the result of an outer join that didn't return data, this
     * method will instead return <code>null</code>
     *
     * @see DatabaseJoinedQueryBuilder
     * @return A {@link DatabaseRow} associated with the specified alias, or null if the alias was
     *          part of an outer join that didn't return data
     */
    public DatabaseRow table(DatabaseTableAlias alias) throws SQLException {
        if (keys.containsKey(alias) && rs.getObject(keys.get(alias)) == null) {
            return null;
        }
        return table(alias.getAlias());
    }

    /**
     * Extracts a {@link DatabaseRow} for the specified {@link DatabaseTableAlias} belonging to the
     * specified alias. If the alias was the result of an outer join that didn't return data, this
     * method will instead return <code>null</code>
     *
     * @see DatabaseJoinedQueryBuilder
     * @return A {@link DatabaseRow} associated with the specified alias, or null if the alias was
     *          part of an outer join that didn't return data
     */
    public DatabaseRow table(DbContextTableAlias alias) throws SQLException {
        return table(alias.getTableAlias());
    }

    /**
     * Extracts a {@link DatabaseRow} for the specified {@link DatabaseTableAlias} belonging to the
     * specified alias.
     *
     * @see DatabaseJoinedQueryBuilder
     * @throws IllegalArgumentException if the specified table wasn't part of the <code>SELECT ... FROM ...</code>
     * clause
     */
    public DatabaseRow table(String table) {
        Map<String, Integer> columnIndexes = tableColumnIndexes.get(table.toUpperCase());
        if (columnIndexes == null) {
            throw new IllegalArgumentException("Unknown table " + table.toUpperCase() + " in " + tableColumnIndexes.keySet());
        }
        return new DatabaseRow(rs, tableColumnIndexes.get(table.toUpperCase()), tableColumnIndexes, this.keys);
    }
}
