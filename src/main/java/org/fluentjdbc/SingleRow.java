package org.fluentjdbc;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface SingleRow<TYPE> extends Iterable<TYPE> {

    /**
     * Signifies that no row matched the query. If the variable is accessed, the specified exception is thrown
     */
    static <T, EX extends RuntimeException> SingleRow<T> absent(Supplier<EX> exception) {
        return new Absent<>(exception);
    }

    static <T> SingleRow<T> of(T result) {
        return new Present<>(result);
    }

    @Nonnull
    TYPE orElseThrow();

    boolean isPresent();

    default TYPE get() {
        return orElseThrow();
    }

    default boolean isEmpty() {
        return !isPresent();
    }

    <U> SingleRow<U> map(Function<? super TYPE, ? extends U> mapper);

    void ifPresent(Consumer<? super TYPE> consumer);

    void ifPresentOrElse(Consumer<? super TYPE> consumer, Runnable emptyAction);

    Stream<TYPE> stream();

    class Absent<T, EX extends RuntimeException> implements SingleRow<T> {
        private final Supplier<EX> exception;

        public Absent(Supplier<EX> exception) {
            this.exception = exception;
        }

        @Override
        @Nonnull
        public Iterator<T> iterator() {
            return Collections.emptyIterator();
        }

        @Nonnull
        @Override
        public T orElseThrow() throws EX {
            throw exception.get();
        }

        @Override
        public boolean isPresent() {
            return false;
        }

        @Override
        public <U> SingleRow<U> map(Function<? super T, ? extends U> mapper) {
            return new Absent<>(exception);
        }

        @Override
        public void ifPresent(Consumer<? super T> consumer) {
        }

        @Override
        public void ifPresentOrElse(Consumer<? super T> consumer, Runnable emptyAction) {
            emptyAction.run();
        }

        @Override
        public Stream<T> stream() {
            return Stream.empty();
        }
    }

    class Present<T> implements SingleRow<T> {
        private final T result;

        public Present(@Nonnull T result) {
            this.result = result;
        }

        @Override
        @Nonnull
        public Iterator<T> iterator() {
            return Collections.singletonList(result).iterator();
        }

        @Nonnull
        @Override
        public T orElseThrow() {
            return result;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public <U> SingleRow<U> map(Function<? super T, ? extends U> mapper) {
            return new Present<>(mapper.apply(result));
        }

        @Override
        public void ifPresent(Consumer<? super T> consumer) {
            consumer.accept(result);
        }

        @Override
        public void ifPresentOrElse(Consumer<? super T> consumer, Runnable emptyAction) {
            consumer.accept(result);
        }

        @Override
        public Stream<T> stream() {
            return Stream.of(result);
        }

        @Override
        public String toString() {
            return "SingleRow{" + result + "}";
        }
    }
}
