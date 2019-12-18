package org.fluentjdbc.usage.context;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.UUID;

@ToString
public class Order {
    @Getter
    @Setter
    private UUID orderId;

    @Getter
    @Setter
    private String customerName, customerEmail;
}
