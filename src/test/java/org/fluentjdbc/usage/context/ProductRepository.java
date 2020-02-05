package org.fluentjdbc.usage.context;

import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseSaveBuilder;
import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.DatabaseSimpleQueryBuilder;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DbContext;
import org.fluentjdbc.DbSelectContext;
import org.fluentjdbc.DbTableContext;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class ProductRepository implements Repository<Product, Product.Id> {
    public EnumMap<DatabaseSaveResult.SaveStatus, Integer> syncProducts(List<Product> products) {
        return table.synch(products)
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

    private static class DatabaseSaveBuilderWithId extends DatabaseSaveBuilder<Product.Id> {

        public DatabaseSaveBuilderWithId(DatabaseTable table, @Nullable Product.Id id) {
            super(table, "product_id", id);
        }

        @Nullable
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
    private DbTableContext table;

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
    public Product retrieve(Product.Id productId) {
        return table.where("product_id", productId.getValue()).singleObject(this::toProduct);
    }

    @Override
    public Query query() {
        return new Query(table.query());
    }

    public class Query implements Repository.Query<Product> {
        private DbSelectContext query;

        public Query(DbSelectContext query) {
            this.query = query;
        }

        @Override
        public Stream<Product> stream() {
            return query.stream(ProductRepository.this::toProduct);
        }
    }

    private Product toProduct(DatabaseRow row) throws SQLException {
        Product product = new Product();
        product.setProductId(new Product.Id(row.getUUID("product_id")));
        product.setName(row.getString("name"));
        product.setCategory(row.getString("category"));
        product.setPriceInCents(row.getInt("price_in_cents"));
        product.setLaunchedAt(row.getInstant("launched_at"));
        return product;
    }
}
