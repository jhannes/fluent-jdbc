package org.fluentjdbc.usage.context;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class ProductSales {
    @Getter
    private final Product.Id productId;

    @Getter
    private final int totalQuantity;

}
