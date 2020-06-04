package org.fluentjdbc.usage.context;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class OrderLineEntity {
    @Getter
    private final Order order;

    @Getter
    private final Product product;

    @Getter
    private final OrderLine orderLine;
}
