package org.fluentjdbc;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Java6Assertions;
import org.joda.time.DateTime;

public class FluentJdbcAsserts extends Java6Assertions {

    public static class DateTimeAssert extends AbstractAssert<DateTimeAssert, DateTime> {

        protected DateTimeAssert(DateTime actual) {
            super(actual, DateTimeAssert.class);
        }

        public DateTimeAssert isAfter(DateTime start) {
            isNotNull();
            if (!actual.isAfter(start)) {
                failWithMessage("Expected %s to be after %s", actual, start);
            }
            return this;
        }

        public DateTimeAssert isBefore(DateTime start) {
            isNotNull();
            if (!actual.isBefore(start)) {
                failWithMessage("Expected %s to be before %s", actual, start);
            }
            return this;
        }

    }

    public static DateTimeAssert assertThat(DateTime actual) {
        return new DateTimeAssert(actual);
    }

}
