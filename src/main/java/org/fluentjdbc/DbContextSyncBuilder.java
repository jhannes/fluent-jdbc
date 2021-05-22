package org.fluentjdbc;

import org.fluentjdbc.DatabaseSaveResult.SaveStatus;

import javax.annotation.CheckReturnValue;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Combined operations for inserting, updating and deleting rows in a table based on a list of objects.
 * Used to synchronize a database table with an external source such as an API or a CSV-file.
 * {@link DbContextSyncBuilder} assumes it's acceptable to hold the full contents of both the table and
 * the external source in memory during the synchronization, which scales well up to a few 100,000 rows.
 *
 * <p>Generate a {@link DbContextSyncBuilder} with {@link DbContextTable#sync(List)} with the target objects,
 * then call {@link #unique(String, Function)} and {@link #field(String, Function)} to specify the relationship
 * between the objects and the database table. Call {@link #cacheExisting()} to load the table into memory
 * and finally, call one or more of {@link #deleteExtras()}, {@link #insertMissing()} and {@link #updateDiffering()}
 * to execute the synchronization. Use {@link #getStatus()} to get a summary of rows inserted, updated, deleted
 * and unchanged.</p>
 *
 * <h3>Example:</h3>
 *
 * <pre>
 *     public EnumMap&lt;DatabaseSaveResult.SaveStatus, Integer&gt; syncProducts(List&lt;Product&gt; products) {
 *         return table.sync(products)
 *                 .unique("product_id", p -&gt; p.getProductId().getValue())
 *                 .field("name", Product::getName)
 *                 .field("category", Product::getCategory)
 *                 .field("price_in_cents", Product::getPriceInCents)
 *                 .field("launched_at", Product::getLaunchedAt)
 *                 .cacheExisting()
 *                 .deleteExtras()
 *                 .insertMissing()
 *                 .updateDiffering()
 *                 .getStatus();
 *     }
 * </pre>
 */
public class DbContextSyncBuilder<T>  {
    private final DbContextTable table;
    private final EnumMap<SaveStatus, Integer> status = new EnumMap<>(SaveStatus.class);
    private final List<T> theirObjects;
    private boolean isCached = false;
    private Map<List<Object>, List<Object>> ourRows;
    private Map<List<Object>, List<Object>> theirRows;
    private final List<String> uniqueFields = new ArrayList<>();
    private final List<Function<T, Object>> uniqueValueFunctions = new ArrayList<>();
    private final List<String> updatedFields = new ArrayList<>();
    private final List<Function<T, Object>> updatedValueFunctions = new ArrayList<>();

    public DbContextSyncBuilder(DbContextTable table, List<T> theirObjects) {
        this.table = table;
        this.theirObjects = theirObjects;
        Stream.of(SaveStatus.values()).forEach(v -> status.put(v, 0));
    }

    /**
     * Specifies that the field is a unique constraint and can be used to look up rows in the database
     * based on objects in memory. valueFunction is used to extract the value from the in-memory object.
     * If unique is called several times, {@link DbContextSyncBuilder} assumes a composite unique key.
     * <em>All unique fields must have a correct equals and hashCode</em>
     */
    @CheckReturnValue
    public DbContextSyncBuilder<T> unique(String field, Function<T, Object> valueFunction) {
        uniqueFields.add(field);
        uniqueValueFunctions.add(valueFunction);
        return this;
    }

    /**
     * Specifies how a column in the database relates to a value extracted from an in-memory object.
     * Call this method for each non-unique field in the database that should be included in the
     * synchronization
     */
    @CheckReturnValue
    public DbContextSyncBuilder<T> field(String field, Function<T, Object> valueFunction) {
        updatedFields.add(field);
        updatedValueFunctions.add(valueFunction);
        return this;
    }

    /**
     * Loads all rows from the database. If called again, this method does nothing
     */
    public DbContextSyncBuilder<T> cacheExisting() {
        if (isCached) {
            return this;
        }
        Map<List<Object>, List<Object>> ourRows = new HashMap<>();
        table.query().forEach(row -> {
            List<Object> key = new ArrayList<>();
            for (String field : uniqueFields) {
                key.add(row.getObject(field));
            }
            List<Object> fields = new ArrayList<>();
            for (String field : updatedFields) {
                fields.add(row.getObject(field));
            }
            ourRows.put(key, fields);
        });
        this.ourRows = ourRows;

        Map<List<Object>, List<Object>> theirRows = new HashMap<>();
        theirObjects.forEach(entity -> {
            List<Object> keys = uniqueValueFunctions.stream()
                    .map(function -> DatabaseStatement.toDatabaseType(function.apply(entity), table.getConnection()))
                    .collect(Collectors.toList());
            theirRows.put(keys, updatedValueFunctions.stream()
                    .map(function -> DatabaseStatement.toDatabaseType(function.apply(entity), table.getConnection()))
                    .collect(Collectors.toList()));
        });
        this.theirRows = theirRows;
        isCached = true;

        return this;
    }

    /**
     * Deletes from the database rows that are not included in the in-memory dataset, using
     * {@link DbContextBulkDeleteBuilder}
     */
    public DbContextSyncBuilder<T> deleteExtras() {
        cacheExisting();
        int count = table.bulkDelete(this.ourRows.keySet().stream()
                .filter(key -> !theirRows.containsKey(key)))
                .whereAll(uniqueFields, entry -> entry)
                .execute();
        status.put(SaveStatus.DELETED, count);
        return this;
    }

    /**
     * Inserts into the database rows that were included in the in-memory dataset, but
     * that didn't have corresponding rows in the database, using {@link DbContextBulkInsertBuilder}
     */
    public DbContextSyncBuilder<T> insertMissing() {
        cacheExisting();
        int count = table.bulkInsert(this.theirRows.entrySet().stream()
                .filter(entry -> !ourRows.containsKey(entry.getKey())))
                .setFields(uniqueFields, Map.Entry::getKey)
                .setFields(updatedFields, Map.Entry::getValue)
                .execute();
        status.put(SaveStatus.INSERTED, count);
        return this;
    }

    /**
     * Updates in the database rows that were different in the in-memory dataset from
     * the database, using {@link DbContextBulkUpdateBuilder}
     */
    public DbContextSyncBuilder<T> updateDiffering() {
        cacheExisting();
        table.bulkUpdate(this.theirRows.entrySet().stream()
                .filter(entry -> ourRows.containsKey(entry.getKey()))
                .filter(entry -> {
                    boolean equal = valuesEqual(entry.getKey());
                    if (equal) {
                        addStatus(SaveStatus.UNCHANGED);
                    } else {
                        addStatus(SaveStatus.UPDATED);
                    }
                    return !equal;
                }))
                .whereAll(uniqueFields, Map.Entry::getKey)
                .setFields(updatedFields, Map.Entry::getValue)
                .execute();
        return this;
    }

    /**
     * Used to compare rows a row in the database with a row in the in-memory dataset by
     * looking them up with the key. The objects in the key must match the fields specified
     * with {@link #unique(String, Function)}
     */
    @CheckReturnValue
    protected boolean valuesEqual(List<Object> key) {
        return areEqualLists(ourRows.get(key), theirRows.get(key));
    }

    /**
     * Compares the fields in two objects, most commonly an in-memory object and an object retrieved
     * from the database with {@link #cacheExisting()}. Override this for example if you have columns
     * that should not be included in the comparison
     */
    @CheckReturnValue
    protected boolean areEqualLists(List<?> a, List<?> b) {
        if (a == null) {
            return b == null;
        } else if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!areEqual(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares a single field for equality. Override this if you have data types that should have special
     * comparison rules
     */
    @CheckReturnValue
    protected boolean areEqual(Object o, Object o1) {
        if (o instanceof BigDecimal) {
            if (!(o1 instanceof BigDecimal)) {
                return false;
            }
            return ((BigDecimal)o).compareTo((BigDecimal)o1) == 0;
        }
        return Objects.equals(o, o1);
    }

    private void addStatus(SaveStatus status) {
        this.status.put(status, this.status.get(status) + 1);
    }

    /**
     * Returns the number of rows that were {@link SaveStatus#UPDATED}, {@link SaveStatus#INSERTED},
     * {@link SaveStatus#DELETED} and {@link SaveStatus#UNCHANGED}
     */
    @CheckReturnValue
    public EnumMap<SaveStatus, Integer> getStatus() {
        return status;
    }
}
