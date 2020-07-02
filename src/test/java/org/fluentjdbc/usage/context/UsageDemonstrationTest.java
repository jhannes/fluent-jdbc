package org.fluentjdbc.usage.context;

import org.fluentjdbc.AbstractDatabaseTest;
import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class UsageDemonstrationTest {

    private final Random random = new Random();

    @Rule
    public final DbContextRule dbContext;

    private final Map<String, String> replacements;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;
    private final OrderLineRepository orderLineRepository;
    private boolean databaseSupportsResultsetMetadataTableName = true;

    public UsageDemonstrationTest() {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected UsageDemonstrationTest(DataSource dataSource, Map<String, String> replacements) {
        this.dbContext = new DbContextRule(dataSource);
        this.replacements = replacements;
        this.productRepository = new ProductRepository(dbContext);
        this.orderRepository = new OrderRepository(dbContext);
        this.orderLineRepository = new OrderLineRepository(dbContext);
    }

    protected void databaseDoesNotSupportResultsetMetadataTableName() {
        databaseSupportsResultsetMetadataTableName = false;
    }

    @Before
    public void createTables() throws SQLException {
        try (Statement statement = dbContext.getThreadConnection().createStatement()) {
            dropTableIfExists(statement, "order_lines");
            dropTableIfExists(statement, "products");
            dropTableIfExists(statement, "orders");
            statement.executeUpdate(preprocessCreateTable(ProductRepository.CREATE_TABLE));
            statement.executeUpdate(preprocessCreateTable(OrderRepository.CREATE_TABLE));
            statement.executeUpdate(preprocessCreateTable(OrderLineRepository.CREATE_TABLE));
        }
    }

    protected void dropTableIfExists(Statement stmt, String tableName) {
        try {
            stmt.executeUpdate("drop table " + tableName);
        } catch(SQLException ignored) {
        }
    }


    @Test
    public void shouldFindSavedRow() {
        Product sampleProduct = sampleProduct();
        productRepository.save(sampleProduct);
        assertThat(productRepository.query().stream())
                .extracting(Product::getName)
                .contains(sampleProduct.getName());
    }

    @Test
    public void shouldRetrieveSavedRow() {
        Product product = sampleProduct();
        productRepository.save(product);
        assertThat(product).hasNoNullFieldsOrProperties();
        assertThat(productRepository.retrieve(product.getProductId())).get()
                .isEqualToComparingFieldByField(product);
    }

    @Test
    public void shouldUpdateSavedRow() {
        Product originalProduct = sampleProduct();
        productRepository.save(originalProduct);

        Product updatedProduct = sampleProduct();
        updatedProduct.setProductId(originalProduct.getProductId());
        productRepository.save(updatedProduct);

        assertThat(productRepository.retrieve(originalProduct.getProductId())).get()
                .isEqualToComparingFieldByField(updatedProduct);
    }

    @Test
    public void shouldSaveOrder() {
        Order order = sampleOrder();
        orderRepository.save(order);
        assertThat(orderRepository.query().customerEmail(order.getCustomerEmail()).stream())
                .extracting(Order::getOrderId)
                .contains(order.getOrderId());
    }

    @Test
    public void shouldUpdateOrder() {
        Order originalOrder = sampleOrder();
        orderRepository.save(originalOrder);
        Order updatedOrder = sampleOrder();
        updatedOrder.setOrderId(originalOrder.getOrderId());
        orderRepository.save(updatedOrder);
        assertThat(orderRepository.retrieve(originalOrder.getOrderId())).get()
                .hasNoNullFieldsOrProperties()
                .isEqualToComparingFieldByField(updatedOrder);
    }

    @Test
    public void shouldSyncProducts() {
        Product changedProduct = sampleProduct();
        Product unchangedProduct = sampleProduct();
        Product deletedProduct = sampleProduct();

        productRepository.save(changedProduct);
        productRepository.save(unchangedProduct);
        productRepository.save(deletedProduct);

        changedProduct.setName("Updated");
        Product newProduct = sampleProduct();
        newProduct.setProductId(new Product.Id(UUID.randomUUID()));

        EnumMap<DatabaseSaveResult.SaveStatus, Integer> syncStatus = productRepository
                .syncProducts(Arrays.asList(unchangedProduct, changedProduct, newProduct));

        verifySyncStatus(syncStatus);
        assertThat(productRepository.query().stream())
                .extracting(Product::getName)
                .containsExactlyInAnyOrder(changedProduct.getName(), unchangedProduct.getName(), newProduct.getName());
    }

    @Test
    public void shouldJoinTables() {
        Assume.assumeTrue("Database vendor does not support ResultSetMetadata.getTableName",
                databaseSupportsResultsetMetadataTableName);

        Product firstProduct = sampleProduct();
        Product secondProduct = sampleProduct();
        Product thirdProduct = sampleProduct();

        productRepository.save(firstProduct);
        productRepository.save(secondProduct);
        productRepository.save(thirdProduct);

        Order order = sampleOrder();
        orderRepository.save(order);

        orderLineRepository.save(new OrderLine(order, firstProduct, 10));
        orderLineRepository.save(new OrderLine(order, secondProduct, 1));
        orderLineRepository.save(new OrderLine(order, thirdProduct, 5));

        assertThat(orderLineRepository.query()
                .orderEmail(order.getCustomerEmail()).list()).extracting(OrderLine::getProductId)
                .contains(firstProduct.getProductId(), secondProduct.getProductId());

        List<OrderLineEntity> entities = orderLineRepository.query()
                .orderEmail(order.getCustomerEmail())
                .productIds(Arrays.asList(firstProduct.getProductId(), secondProduct.getProductId()))
                .listEntities();

        assertThat(entities).extracting(o -> o.getProduct().getName())
                .contains(firstProduct.getName(), secondProduct.getName())
                .doesNotContain(thirdProduct.getName());
        assertThat(entities).extracting(o -> o.getOrder().getOrderId()).containsOnly(order.getOrderId());
    }

    @Test
    public void shouldPerformLeftJoin() {
        Assume.assumeTrue("Database vendor does not support ResultSetMetadata.getTableName",
                databaseSupportsResultsetMetadataTableName);

        Product firstProduct = sampleProduct();
        Product secondProduct = sampleProduct();
        Product thirdProduct = sampleProduct();

        productRepository.save(firstProduct);
        productRepository.save(secondProduct);
        productRepository.save(thirdProduct);

        Order firstOrder = sampleOrder();
        Order secondOrder = sampleOrder();
        orderRepository.save(firstOrder);
        orderRepository.save(secondOrder);

        orderLineRepository.save(new OrderLine(firstOrder, firstProduct, 10));
        orderLineRepository.save(new OrderLine(firstOrder, secondProduct, 1));
        orderLineRepository.save(new OrderLine(secondOrder, firstProduct, 5));

        Collection<ProductSales> productSales = productRepository.salesReport();
        assertThat(productSales)
                .filteredOn(p -> p.getProductId().equals(firstProduct.getProductId()))
                .extracting(ProductSales::getTotalQuantity)
                .containsExactly(10+5);
        assertThat(productSales)
                .filteredOn(p -> p.getProductId().equals(thirdProduct.getProductId()))
                .extracting(ProductSales::getTotalQuantity)
                .containsExactly(0);
    }

    protected void verifySyncStatus(EnumMap<DatabaseSaveResult.SaveStatus, Integer> syncStatus) {
        assertThat(syncStatus)
                .containsEntry(DatabaseSaveResult.SaveStatus.UNCHANGED, 1)
                .containsEntry(DatabaseSaveResult.SaveStatus.UPDATED, 1)
                .containsEntry(DatabaseSaveResult.SaveStatus.DELETED, 1)
                .containsEntry(DatabaseSaveResult.SaveStatus.INSERTED, 1);
    }

    private Order sampleOrder() {
        Order order = new Order();
        order.setCustomerEmail(pickOne("alice", "bob", "charlie", "diana")
                + "@example" + random.nextInt() + pickOne(".net", ".com", ".org"));
        order.setCustomerName(pickOne("Alice", "Bob", "Charlie", "Diana")
                + " " + pickOne("Adams", "Smith", "Jones", "Doe"));
        order.setOrderTime(OffsetDateTime.now().truncatedTo(ChronoUnit.SECONDS));
        order.setLoyaltyPercentage(0.25);
        return order;
    }

    private Product sampleProduct() {
        Product product = new Product();
        product.setName(pickOne("Apples", "Bananas", "Coconuts", "Doritos") + " " + UUID.randomUUID());
        product.setCategory(pickOne("fruit", "breakfast", "dairy", "non-food", "literature", "candy"));
        product.setPriceInCents(1000 + random.nextInt(1000) * 10);
        product.setLaunchedAt(Instant.now().truncatedTo(ChronoUnit.SECONDS));
        return product;
    }

    @SafeVarargs
    private final <T> T pickOne(T... alternatives) {
        return alternatives[random.nextInt(alternatives.length)];
    }

    protected String preprocessCreateTable(String createTableStatement) {
        return AbstractDatabaseTest.preprocessCreateTable(createTableStatement, replacements);
    }

}
