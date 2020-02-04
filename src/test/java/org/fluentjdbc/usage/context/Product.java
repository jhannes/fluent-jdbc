package org.fluentjdbc.usage.context;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.UUID;

@ToString
public class Product {

    @ToString
    @RequiredArgsConstructor
    @EqualsAndHashCode
    public static class Id {
        @Getter
        private final UUID value;
    }


    @Getter
    @Setter
    private Id productId;

    @Getter
    @Setter
    private String name, category;

    @Getter
    @Setter
    private int priceInCents;

    @Getter
    @Setter
    private Instant launchedAt;
}
