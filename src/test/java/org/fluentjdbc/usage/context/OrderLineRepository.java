package org.fluentjdbc.usage.context;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.DbContext;
import org.fluentjdbc.DbContextJoinedSelectBuilder;
import org.fluentjdbc.DbContextTableAlias;
import org.fluentjdbc.DbContextTable;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrderLineRepository implements Repository<OrderLine, UUID> {
    public static final String CREATE_TABLE = "CREATE TABLE order_lines (" +
            "id ${UUID} primary key, " +
            "order_id ${UUID} not null references orders(order_id), " +
            "product_id ${UUID} not null references products(product_id), " +
            "quantity integer not null, " +
            "updated_at ${DATETIME} not null, " +
            "created_at ${DATETIME} not null)";
    private final DbContextTable table;
    private final DbContextTable productsTable;
    private final DbContextTable ordersTable;

    public OrderLineRepository(DbContext dbContext) {
        this.table = dbContext.tableWithTimestamps("order_lines");
        this.productsTable = dbContext.tableWithTimestamps("products");
        this.ordersTable = dbContext.tableWithTimestamps("orders");
    }

    @Override
    public DatabaseSaveResult.SaveStatus save(OrderLine row) {
        DatabaseSaveResult<UUID> result = table.newSaveBuilderWithUUID("id", row.getId())
                .setField("product_id", row.getProductId().getValue())
                .setField("order_id", row.getOrderId())
                .setField("quantity", row.getQuantity())
                .execute();
        row.setId(result.getId());
        return result.getSaveStatus();
    }

    static OrderLine toOrderLine(DatabaseRow row) throws SQLException {
        OrderLine line = new OrderLine();
        line.setId(row.getUUID("id"));
        line.setOrderId(row.getUUID("order_id"));
        line.setProductId(new Product.Id(row.getUUID("product_id")));
        line.setQuantity(row.getInt("quantity"));
        return line;
    }

    @Override
    public QueryImpl query() {
        return new QueryImpl();
    }

    @Override
    public Optional<OrderLine> retrieve(UUID uuid) {
        return Optional.empty();
    }

    public class QueryImpl implements Query<OrderLine> {
        private final DbContextJoinedSelectBuilder context;
        final DbContextTableAlias linesAlias = table.alias("l");

        public QueryImpl() {
            this.context = linesAlias.select();
        }

        @Override
        public Stream<OrderLine> stream() {
            return null;
        }

        public QueryImpl orderEmail(String customerEmail) {
            return query(context.whereExpression("l.order_id in (select o.order_id from orders o where customer_email = ?)", customerEmail));
        }

        public QueryImpl productIds(List<Product.Id> productIds) {
            return query(context.whereIn("l.product_id", productIds.stream().map(Product.Id::getValue).collect(Collectors.toList())));
        }

        public List<OrderLineEntity> listEntities() {
            DbContextTableAlias p = productsTable.alias("p");
            DbContextTableAlias o = ordersTable.alias("o");
            return context
                    .join(linesAlias.column("product_id"), p.column("product_id"))
                    .join(linesAlias.column("order_id"), o.column("order_id"))
                    .list(row -> new OrderLineEntity(
                            OrderRepository.toOrder(row.table(o)),
                            ProductRepository.toProduct(row.table(p)),
                            toOrderLine(row.table(linesAlias))
                    ));
        }

        public List<OrderLine> list() {
            return context.list(OrderLineRepository::toOrderLine);
        }

        private QueryImpl query(DbContextJoinedSelectBuilder context) {
            return this;
        }

    }
}
