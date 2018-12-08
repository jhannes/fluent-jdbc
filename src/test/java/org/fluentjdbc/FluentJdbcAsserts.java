package org.fluentjdbc;

import java.time.Instant;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Java6Assertions;

public class FluentJdbcAsserts extends Java6Assertions {

    public static class DateTimeAssert extends AbstractAssert<DateTimeAssert, Instant> {

        protected DateTimeAssert(Instant actual) {
            super(actual, DateTimeAssert.class);
        }

        public DateTimeAssert isAfter(Instant start) {
            isNotNull();
            if (!actual.isAfter(start)) {
                failWithMessage("Expected %s to be after %s", actual, start);
            }
            return this;
        }

        public DateTimeAssert isBefore(Instant start) {
            isNotNull();
            if (!actual.isBefore(start)) {
                failWithMessage("Expected %s to be before %s", actual, start);
            }
            return this;
        }

    }

    public static DateTimeAssert assertThat(Instant actual) {
        return new DateTimeAssert(actual);
    }

}
