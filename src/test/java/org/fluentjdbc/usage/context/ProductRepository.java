package org.fluentjdbc.usage.context;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseSaveBuilder;
import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.DatabaseSimpleQueryBuilder;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DatabaseTableAlias;
import org.fluentjdbc.DbContext;
import org.fluentjdbc.DbContextSelectBuilder;
import org.fluentjdbc.DbContextTableAlias;
import org.fluentjdbc.DbContextTable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public class ProductRepository implements Repository<Product, Product.Id> {
    public EnumMap<DatabaseSaveResult.SaveStatus, Integer> syncProducts(List<Product> products) {
        return table.sync(products)
                .unique("product_id", p -> p.getProductId().getValue())
                .field("name", Product::getName)
                .field("category", Product::getCategory)
                .field("price_in_cents", Product::getPriceInCents)
                .field("launched_at", Product::getLaunchedAt)
                .cacheExisting()
                .deleteExtras()
                .insertMissing()
                .updateDiffering()
                .getStatus();
    }

    public Collection<ProductSales> salesReport() {
        DatabaseTableAlias linesAlias = new DatabaseTableAlias("order_lines", "l");
        DbContextTableAlias productAlias = table.alias("p");
        Map<Product.Id, ProductSales> report = new HashMap<>();
        productAlias
                .leftJoin(productAlias.column("product_id"), linesAlias.column("product_id"))
                .forEach(row -> {
                    Product product = toProduct(row.table(productAlias));
                    ProductSales productSales = report.computeIfAbsent(product.getProductId(), ProductSales::new);
                    row.table(linesAlias, OrderLineRepository::toOrderLine).ifPresent(productSales::addSale);
                });
        return report.values();
    }

    private static class DatabaseSaveBuilderWithId extends DatabaseSaveBuilder<Product.Id> {

        public DatabaseSaveBuilderWithId(DatabaseTable table, @Nullable Product.Id id) {
            super(table, "product_id", id);
        }

        @Nonnull
        @Override
        protected Product.Id insert(Connection connection) {
            Product.Id idValue = this.idValue;
            if (idValue == null) {
                idValue = new Product.Id(UUID.randomUUID());
            }
            table.insert()
                    .setField(idField, idValue.getValue())
                    .setFields(fields, values)
                    .setFields(uniqueKeyFields, uniqueKeyValues)
                    .execute(connection);
            return idValue;
        }

        @Override
        protected Product.Id getId(DatabaseRow row) throws SQLException {
            return new Product.Id(row.getUUID(idField));
        }

        @Override
        protected DatabaseSimpleQueryBuilder tableWhereId(Product.Id id) {
            return table.where(idField, id.getValue());
        }
    }


    public static final String CREATE_TABLE = "CREATE TABLE products (" +
            "product_id ${UUID} primary key, " +
            "name varchar(200) not null, " +
            "category varchar(200), " +
            "price_in_cents integer, " +
            "launched_at ${DATETIME}, " +
            "updated_at ${DATETIME} not null, " +
            "created_at ${DATETIME} not null)";
    private final DbContextTable table;

    public ProductRepository(DbContext dbContext) {
        table = dbContext.tableWithTimestamps("products");
    }

    @Override
    public DatabaseSaveResult.SaveStatus save(Product product) {
        DatabaseSaveResult<Product.Id> result = table.save(new DatabaseSaveBuilderWithId(table.getTable(), product.getProductId()))
                .setField("name", product.getName())
                .setField("category", product.getCategory())
                .setField("price_in_cents", product.getPriceInCents())
                .setField("launched_at", product.getLaunchedAt())
                .execute();
        product.setProductId(result.getId());
        return result.getSaveStatus();
    }

    @Override
    public Optional<Product> retrieve(Product.Id productId) {
        return table.where("product_id", productId.getValue()).singleObject(ProductRepository::toProduct);
    }

    @Override
    public Query query() {
        return new Query(table.query());
    }

    public static class Query implements Repository.Query<Product> {
        private final DbContextSelectBuilder query;

        public Query(DbContextSelectBuilder query) {
            this.query = query;
        }

        @Override
        public Stream<Product> stream() {
            return query.stream(ProductRepository::toProduct);
        }
    }

    static Product toProduct(DatabaseRow row) throws SQLException {
        Product product = new Product();
        product.setProductId(new Product.Id(row.getUUID("product_id")));
        product.setName(row.getString("name"));
        product.setCategory(row.getString("category"));
        product.setPriceInCents(row.getInt("price_in_cents"));
        product.setLaunchedAt(row.getInstant("launched_at"));
        return product;
    }
}
