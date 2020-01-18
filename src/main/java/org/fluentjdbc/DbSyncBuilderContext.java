package org.fluentjdbc;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbSyncBuilderContext<T>  {
    private final DbTableContext table;
    private final List<T> entities;
    private List<String> uniqueFields = new ArrayList<>();
    private List<Function<T, Object>> uniqueValueFunctions = new ArrayList<>();
    private List<String> updatedFields = new ArrayList<>();
    private List<Function<T, Object>> updatedValueFunctions = new ArrayList<>();
    private Map<List<Object>, List<Object>> existingRows;
    private Map<List<Object>, List<Object>> updated;
    private EnumMap<DatabaseSaveResult.SaveStatus, Integer> status = new EnumMap<>(DatabaseSaveResult.SaveStatus.class);

    public DbSyncBuilderContext(DbTableContext table, List<T> entities) {
        this.table = table;
        this.entities = entities;
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
        Map<List<Object>, List<Object>> existing = new HashMap<>();
        table.query().forEach(row -> {
            List<Object> key = new ArrayList<>();
            for (String field : uniqueFields) {
                key.add(row.getObject(field));
            }
            List<Object> result = new ArrayList<>();
            for (String field : updatedFields) {
                result.add(row.getObject(field));
            }
            existing.put(key, result);
        });
        this.existingRows = existing;

        Map<List<Object>, List<Object>> updated = new HashMap<>();
        entities.forEach(entity -> {
            List<Object> keys = uniqueValueFunctions.stream().map(f -> f.apply(entity)).collect(Collectors.toList());
            updated.put(keys, updatedValueFunctions.stream()
                    .map(function -> function.apply(entity))
                    .collect(Collectors.toList()));
        });
        this.updated = updated;

        return this;
    }

    public DbSyncBuilderContext<T> deleteExtras() {
        int count = table.bulkDelete(this.existingRows.keySet().stream()
                .filter(key -> !updated.containsKey(key)))
                .whereAll(uniqueFields, entry -> entry)
                .execute();
        status.put(DatabaseSaveResult.SaveStatus.DELETED, count);
        return this;
    }

    public DbSyncBuilderContext<T> insertMissing() {
        int count = table.bulkInsert(this.updated.entrySet().stream()
                .filter(entry -> !existingRows.containsKey(entry.getKey())))
                .setFields(uniqueFields, Map.Entry::getKey)
                .setFields(updatedFields, Map.Entry::getValue)
                .execute();
        status.put(DatabaseSaveResult.SaveStatus.INSERTED, count);
        return this;
    }

    public DbSyncBuilderContext<T> updateDiffering() {
        int count = table.bulkUpdate(this.updated.entrySet().stream()
                .filter(entry -> existingRows.containsKey(entry.getKey()))
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
        return existingRows.get(key).equals(updated.get(key));
    }

    private void addStatus(DatabaseSaveResult.SaveStatus status) {
        this.status.put(status, this.status.get(status) + 1);
    }

    public EnumMap<DatabaseSaveResult.SaveStatus, Integer> getStatus() {
        return status;
    }
}
