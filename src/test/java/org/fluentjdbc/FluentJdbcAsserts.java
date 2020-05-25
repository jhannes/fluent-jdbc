package org.fluentjdbc;

import org.assertj.core.api.AbstractInstantAssert;
import org.assertj.core.api.Assertions;

import java.time.Instant;
import java.util.Optional;

public class FluentJdbcAsserts extends Assertions {

    public static AbstractInstantAssert<?> assertThatOptional(Optional<Instant> actual) {
        assertThat(actual).isPresent();
        return assertThat(actual.get());
    }

}
