package org.fluentjdbc;

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

public class DbSyncBuilderContext<T>  {
    private final DbTableContext table;
    private EnumMap<DatabaseSaveResult.SaveStatus, Integer> status = new EnumMap<>(DatabaseSaveResult.SaveStatus.class);
    private final List<T> theirObjects;
    private boolean isCached = false;
    private Map<List<Object>, List<Object>> ourRows;
    private Map<List<Object>, List<Object>> theirRows;
    private List<String> uniqueFields = new ArrayList<>();
    private List<Function<T, Object>> uniqueValueFunctions = new ArrayList<>();
    private List<String> updatedFields = new ArrayList<>();
    private List<Function<T, Object>> updatedValueFunctions = new ArrayList<>();

    public DbSyncBuilderContext(DbTableContext table, List<T> theirObjects) {
        this.table = table;
        this.theirObjects = theirObjects;
        Stream.of(DatabaseSaveResult.SaveStatus.values()).forEach(v -> status.put(v, 0));
    }

    public DbSyncBuilderContext<T> unique(String field, Function<T, Object> valueFunction) {
        uniqueFields.add(field);
        uniqueValueFunctions.add(valueFunction);
        return this;
    }

    public DbSyncBuilderContext<T> field(String field, Function<T, Object> valueFunction) {
        updatedFields.add(field);
        updatedValueFunctions.add(valueFunction);
        return this;
    }

    public DbSyncBuilderContext<T> cacheExisting() {
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

    public DbSyncBuilderContext<T> deleteExtras() {
        cacheExisting();
        int count = table.bulkDelete(this.ourRows.keySet().stream()
                .filter(key -> !theirRows.containsKey(key)))
                .whereAll(uniqueFields, entry -> entry)
                .execute();
        status.put(DatabaseSaveResult.SaveStatus.DELETED, count);
        return this;
    }

    public DbSyncBuilderContext<T> insertMissing() {
        cacheExisting();
        int count = table.bulkInsert(this.theirRows.entrySet().stream()
                .filter(entry -> !ourRows.containsKey(entry.getKey())))
                .setFields(uniqueFields, Map.Entry::getKey)
                .setFields(updatedFields, Map.Entry::getValue)
                .execute();
        status.put(DatabaseSaveResult.SaveStatus.INSERTED, count);
        return this;
    }

    public DbSyncBuilderContext<T> updateDiffering() {
        cacheExisting();
        int count = table.bulkUpdate(this.theirRows.entrySet().stream()
                .filter(entry -> ourRows.containsKey(entry.getKey()))
                .filter(entry -> {
                    boolean equal = valuesEqual(entry.getKey());
                    if (equal) {
                        addStatus(DatabaseSaveResult.SaveStatus.UNCHANGED);
                    }
                    return !equal;
                }))
                .whereAll(uniqueFields, Map.Entry::getKey)
                .setFields(updatedFields, Map.Entry::getValue)
                .execute();
        status.put(DatabaseSaveResult.SaveStatus.UPDATED, count);

        return this;
    }

    protected boolean valuesEqual(List<Object> key) {
        return areEqual(ourRows.get(key), theirRows.get(key));
    }

    private boolean areEqual(List<Object> a, List<Object> b) {
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

    private boolean areEqual(Object o, Object o1) {
        if (o instanceof BigDecimal) {
            if (!(o1 instanceof BigDecimal)) {
                return false;
            }
            return ((BigDecimal)o).compareTo((BigDecimal)o1) == 0;
        }
        return Objects.equals(o, o1);
    }

    private void addStatus(DatabaseSaveResult.SaveStatus status) {
        this.status.put(status, this.status.get(status) + 1);
    }

    public EnumMap<DatabaseSaveResult.SaveStatus, Integer> getStatus() {
        return status;
    }
}
