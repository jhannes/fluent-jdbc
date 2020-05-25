package org.fluentjdbc.usage.context;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.OffsetDateTime;
import java.util.UUID;

@ToString
public class Order {
    @Getter
    @Setter
    private UUID orderId;

    @Getter
    @Setter
    private String customerName, customerEmail;

    @Getter
    @Setter
    private OffsetDateTime orderTime;

    @Getter
    @Setter
    private double loyaltyPercentage;
}
