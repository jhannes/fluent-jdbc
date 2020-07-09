package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DatabaseSaveResult<T> {
    private final T idValue;
    private final SaveStatus saveStatus;
    private final List<String> updatedFields;

    private DatabaseSaveResult(T idValue, SaveStatus saveStatus, List<String> updatedFields) {
        this.idValue = idValue;
        this.saveStatus = saveStatus;
        this.updatedFields = updatedFields;
    }

    public boolean isChanged() {
        return saveStatus != SaveStatus.UNCHANGED;
    }

    public static <T> DatabaseSaveResult<T> updated(@Nonnull T idValue, List<String> updatedFields) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.UPDATED, updatedFields);
    }

    public static <T> DatabaseSaveResult<T> inserted(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.INSERTED, new ArrayList<>());
    }

    public static <T> DatabaseSaveResult<T> deleted(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.DELETED, new ArrayList<>());
    }

    public static <T> DatabaseSaveResult<T> unchanged(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.UNCHANGED, new ArrayList<>());
    }

    public T getId() {
        return idValue;
    }

    public SaveStatus getSaveStatus() {
        return saveStatus;
    }

    public List<String> getUpdatedFields() {
        return updatedFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseSaveResult<?> that = (DatabaseSaveResult<?>) o;
        return Objects.equals(idValue, that.idValue) &&
                saveStatus == that.saveStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(idValue, saveStatus);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{idValue=" + idValue + ", saveStatus=" + saveStatus + ", updatedFields=" + updatedFields + '}';
    }

    public enum SaveStatus {
        INSERTED, UNCHANGED, DELETED, UPDATED
    }
}
