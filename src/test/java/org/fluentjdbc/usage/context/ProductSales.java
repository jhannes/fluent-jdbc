package org.fluentjdbc.usage.context;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class ProductSales {
    @Getter
    private final Product.Id productId;

    @Getter
    @Setter
    private int totalQuantity;

    public void addSale(OrderLine orderLine) {
        totalQuantity += orderLine.getQuantity();
    }
}
