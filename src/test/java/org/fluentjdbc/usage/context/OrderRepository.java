package org.fluentjdbc.usage.context;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.DbContext;
import org.fluentjdbc.DbContextSelectBuilder;
import org.fluentjdbc.DbContextTable;
import org.fluentjdbc.SingleRow;

import java.sql.SQLException;
import java.util.UUID;
import java.util.stream.Stream;

public class OrderRepository implements Repository<Order, UUID> {
    public static final String CREATE_TABLE = "CREATE TABLE orders (" +
            "order_id ${UUID} primary key, " +
            "customer_name varchar(200) not null, " +
            "customer_email varchar(200) not null, " +
            "loyalty_percentage numeric(10,5) not null, " +
            "order_time ${DATETIME}, " +
            "updated_at ${DATETIME} not null, " +
            "created_at ${DATETIME} not null)";

    private final DbContextTable table;

    public OrderRepository(DbContext dbContext) {
        this.table = dbContext.tableWithTimestamps("orders");
    }

    @Override
    public DatabaseSaveResult.SaveStatus save(Order order) {
        DatabaseSaveResult<UUID> result = table.newSaveBuilderWithUUID("order_id", order.getOrderId())
                .setField("customer_name", order.getCustomerName())
                .setField("customer_email", order.getCustomerEmail())
                .setField("order_time", order.getOrderTime())
                .setField("loyalty_percentage", order.getLoyaltyPercentage())
                .execute();
        order.setOrderId(result.getId());
        return result.getSaveStatus();
    }

    @Override
    public Query query() {
        return new Query(table.query());
    }

    @Override
    public SingleRow<Order> retrieve(UUID uuid) {
        return table.where("order_id", uuid).singleObject(OrderRepository::toOrder);
    }

    public static class Query implements Repository.Query<Order> {

        private final DbContextSelectBuilder context;

        public Query(DbContextSelectBuilder context) {
            this.context = context;
        }

        @Override
        public Stream<Order> stream() {
            return context.stream(OrderRepository::toOrder);
        }

        public Query customerEmail(String customerEmail) {
            return query(context.where("customer_email", customerEmail));
        }

        private Query query(@SuppressWarnings("unused") DbContextSelectBuilder context) {
            return this;
        }
    }

    static Order toOrder(DatabaseRow row) throws SQLException {
        Order order = new Order();
        order.setOrderId(row.getUUID("order_id"));
        order.setCustomerName(row.getString("customer_name"));
        order.setCustomerEmail(row.getString("customer_email"));
        order.setOrderTime(row.getOffsetDateTime("order_time"));
        order.setLoyaltyPercentage(row.getDouble("loyalty_percentage"));
        return order;
    }
}
