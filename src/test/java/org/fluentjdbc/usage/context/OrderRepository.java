package org.fluentjdbc.usage.context;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.DbContext;
import org.fluentjdbc.DbSelectContext;
import org.fluentjdbc.DbTableContext;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class OrderRepository implements Repository<Order, UUID> {
    public static final String CREATE_TABLE = "CREATE TABLE orders (" +
            "order_id ${UUID} primary key, " +
            "customer_name varchar(200) not null, " +
            "customer_email varchar(200) not null, " +
            "updated_at ${DATETIME} not null, " +
            "created_at ${DATETIME} not null)";

    private final DbTableContext table;

    public OrderRepository(DbContext dbContext) {
        this.table = dbContext.tableWithTimestamps("orders");
    }

    @Override
    public DatabaseSaveResult.SaveStatus save(Order product) {
        DatabaseSaveResult<UUID> result = table.newSaveBuilderWithUUID("order_id", product.getOrderId())
                .setField("customer_name", product.getCustomerName())
                .setField("customer_email", product.getCustomerEmail())
                .execute();
        product.setOrderId(result.getId());
        return result.getSaveStatus();
    }

    @Override
    public Query query() {
        return new Query(table.query());
    }

    @Override
    public Order retrieve(UUID uuid) {
        return table.where("order_id", uuid).singleObject(this::toOrder);
    }

    public class Query implements Repository.Query<Order> {

        private DbSelectContext context;

        public Query(DbSelectContext context) {
            this.context = context;
        }

        @Override
        public Stream<Order> stream() {
            return context.stream(row -> toOrder(row));
        }

        public Query customerEmail(String customerEmail) {
            return query(context.where("customer_email", customerEmail));
        }

        private Query query(DbSelectContext contex) {
            return this;
        }
    }

    private Order toOrder(DatabaseRow row) throws SQLException {
        Order order = new Order();
        order.setOrderId(row.getUUID("order_id"));
        order.setCustomerName(row.getString("customer_name"));
        order.setCustomerEmail(row.getString("customer_email"));
        return order;
    }
}
