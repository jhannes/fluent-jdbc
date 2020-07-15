package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Encapsulates what operation was executed when data was saved to the database.
 *
 * <ul>
 *     <li>INSERTED: A row was inserted into the database. {@link #getId()} contains the generated
 *     or specified primary key. {@link #getUpdatedFields()} is empty</li>
 *     <li>UPDATED: A row was updated because fluent-jdbc detected that the data was modified.
 *     {@link #getUpdatedFields()} contains the list of fields that were detected as changed.
 *     {@link #getId()} contains the primary key of the updated row</li>
 *     <li>UNCHANGED: Nothing was updated as fluent-jdbc determined that the values in the database
 *     were equal to the specified values. {@link #getId()} contains the primary key of the row
 *     used for comparison</li>
 *     <li>DELETED: The row was deleted. Only supported by {@link DbContextSyncBuilder}</li>
 * </ul>
 * @param <T> The type of the primary key
 */
public class DatabaseSaveResult<T> {
    private final T idValue;
    private final SaveStatus saveStatus;
    private final List<String> updatedFields;

    private DatabaseSaveResult(T idValue, SaveStatus saveStatus, List<String> updatedFields) {
        this.idValue = idValue;
        this.saveStatus = saveStatus;
        this.updatedFields = updatedFields;
    }

    /**
     * Returns true unless the save-operation resulted in no changes.
     */
    public boolean isChanged() {
        return saveStatus != SaveStatus.UNCHANGED;
    }

    /**
     * Returns the primary key of the row that the operation was executed on
     */
    public T getId() {
        return idValue;
    }

    /**
     * Returns the #SaveStatus specifying what operation was executed
     */
    public SaveStatus getSaveStatus() {
        return saveStatus;
    }

    /**
     * Returns a list of column names that were updated in the database. Only returns a
     * value for UPDATED results
     */
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

    /**
     * The type of operation that was executed
     */
    public enum SaveStatus {
        /**
         *  A row was inserted into the database. {@link #getId()} contains the generated
         *  or specified primary key. {@link #getUpdatedFields()} is empty
         */
        INSERTED,
        /**
         * Nothing was updated as fluent-jdbc determined that the values in the database
         * were equal to the specified values. {@link #getId()} contains the primary key of the row
         * used for comparison
         */
        UNCHANGED,
        /**
         * The row was deleted. Only supported by {@link DbContextSyncBuilder}
         */
        DELETED,
        /**
         * A row was updated because fluent-jdbc detected that the data was modified.
         * {@link #getUpdatedFields()} contains the list of fields that were detected as changed.
         * {@link #getId()} contains the primary key of the updated row
         */
        UPDATED
    }

    /**
     * Factory method to create {@link DatabaseResult} with status UPDATED
     */
    public static <T> DatabaseSaveResult<T> updated(@Nonnull T idValue, List<String> updatedFields) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.UPDATED, updatedFields);
    }

    /**
     * Factory method to create {@link DatabaseResult} with status INSERTED
     */
    public static <T> DatabaseSaveResult<T> inserted(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.INSERTED, new ArrayList<>());
    }

    /**
     * Factory method to create {@link DatabaseResult} with status DELETED
     */
    public static <T> DatabaseSaveResult<T> deleted(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.DELETED, new ArrayList<>());
    }

    /**
     * Factory method to create {@link DatabaseResult} with status UNCHANGED
     */
    public static <T> DatabaseSaveResult<T> unchanged(@Nonnull T idValue) {
        return new DatabaseSaveResult<>(idValue, SaveStatus.UNCHANGED, new ArrayList<>());
    }

}
