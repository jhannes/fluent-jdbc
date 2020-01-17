package org.fluentjdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DbSyncBuilderContext<T> {
    private final DbTableContext table;
    private final List<T> entities;
    private List<String> uniqueFields = new ArrayList<>();
    private List<Function<T, Object>> uniqueValueFunctions = new ArrayList<>();
    private List<String> updatedFields = new ArrayList<>();
    private List<Function<T, Object>> updatedValueFunctions = new ArrayList<>();
    private Map<List<Object>, Map<String, Object>> existingRows;
    private Map<List<Object>, Map<String, Object>> updated;

    public DbSyncBuilderContext(DbTableContext table, List<T> entities) {
        this.table = table;
        this.entities = entities;
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
        Map<List<Object>, Map<String, Object>> existing = new HashMap<>();
        table.query().forEach(row -> {
            List<Object> key = new ArrayList<>();
            for (String field : uniqueFields) {
                key.add(row.getObject(field));
            }
            HashMap<String, Object> result = new HashMap<>();
            for (String field : updatedFields) {
                result.put(field, row.getObject(field));
            }
            existing.put(key, result);
        });
        this.existingRows = existing;

        Map<List<Object>, Map<String, Object>> updated = new HashMap<>();
        entities.forEach(entity -> {
            List<Object> keys = uniqueValueFunctions.stream().map(f -> f.apply(entity)).collect(Collectors.toList());
            HashMap<String, Object> result = new HashMap<>();
            for (int i = 0; i < updatedFields.size(); i++) {
                String field = updatedFields.get(i);
                result.put(updatedFields.get(i), updatedValueFunctions.get(i).apply(entity));
            }
            updated.put(keys, result);
        });
        this.updated = updated;

        return this;
    }

    public DbSyncBuilderContext<T> deleteExtras() {
        this.existingRows.keySet().stream()
                .filter(key -> !updated.containsKey(key))
                .forEach(key -> table.whereAll(uniqueFields, key).executeDelete());
        return this;
    }

    public DbSyncBuilderContext<T> insertMissing() {
        this.updated.keySet().stream()
                .filter(key -> !existingRows.containsKey(key))
                .forEach(key -> table.insert()
                        .setFields(uniqueFields, key)
                        .setFields(updated.get(key).keySet(), updated.get(key).values())
                        .execute());
        return this;
    }

    public DbSyncBuilderContext<T> updateDiffering() {
        this.updated.keySet().stream()
                .filter(key -> existingRows.containsKey(key))
                .forEach(key -> table.whereAll(uniqueFields, key)
                        .update()
                        .setFields(updated.get(key).keySet(), updated.get(key).values())
                        .execute());
        return this;
    }
}
