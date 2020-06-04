package org.fluentjdbc.usage.context;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

public class OrderLine {
    @Getter @Setter
    private UUID orderId, id;

    @Getter @Setter
    private Product.Id productId;

    @Getter @Setter
    private int quantity;

    public OrderLine(Order order, Product product, int quantity) {
        this.orderId = order.getOrderId();
        this.productId = product.getProductId();
        this.quantity = quantity;
    }

    public OrderLine() {}
}
