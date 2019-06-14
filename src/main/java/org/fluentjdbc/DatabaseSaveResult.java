package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.util.Objects;

public class DatabaseSaveResult<T> {
    private final T idValue;
    private final SaveStatus saveStatus;

    private DatabaseSaveResult(T idValue, SaveStatus saveStatus) {
        this.idValue = idValue;
        this.saveStatus = saveStatus;
    }

    public static <T> DatabaseSaveResult<T> updated(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.UPDATED);
    }

    public static <T> DatabaseSaveResult<T> inserted(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.INSERTED);
    }

    public static <T> DatabaseSaveResult<T> unchanged(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.UNCHANGED);
    }

    public T getId() {
        return idValue;
    }

    public SaveStatus getSaveStatus() {
        return saveStatus;
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
        return getClass().getSimpleName() + "{idValue=" + idValue + ", saveStatus=" + saveStatus + '}';
    }

    public enum SaveStatus {
        INSERTED, UNCHANGED, UPDATED
    }
}
