package org.fluentjdbc.usage.context;

import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class UsageDemonstrationTest {

    private Random random = new Random();


    @Rule
    public final DbContextRule dbContext;
    private Map<String, String> replacements;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;

    public UsageDemonstrationTest() throws SQLException {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected UsageDemonstrationTest(DataSource dataSource, Map<String, String> replacements) throws SQLException {
        this.dbContext = new DbContextRule(dataSource);
        this.replacements = replacements;
        this.productRepository = new ProductRepository(dbContext);
        this.orderRepository = new OrderRepository(dbContext);
    }

    @Before
    public void createTables() throws SQLException {
        try (Statement statement = dbContext.getThreadConnection().createStatement()) {
            statement.executeUpdate(preprocessCreateTable(ProductRepository.CREATE_TABLE));
            statement.executeUpdate(preprocessCreateTable(OrderRepository.CREATE_TABLE));
        }
    }

    @Test
    public void shouldFindSavedRow() {
        Product sampleProduct = sampleProduct();
        productRepository.save(sampleProduct);
        assertThat(productRepository.query().list())
                .extracting(Product::getName)
                .contains(sampleProduct.getName());
    }

    @Test
    public void shouldRetrieveSavedRow() {
        Product product = sampleProduct();
        productRepository.save(product);
        assertThat(product).hasNoNullFieldsOrProperties();
        assertThat(productRepository.retrieve(product.getProductId()))
                .isEqualToComparingFieldByField(product);
    }

    @Test
    public void shouldUpdateSavedRow() {
        Product originalProduct = sampleProduct();
        productRepository.save(originalProduct);

        Product updatedProduct = sampleProduct();
        updatedProduct.setProductId(originalProduct.getProductId());
        productRepository.save(updatedProduct);

        assertThat(productRepository.retrieve(originalProduct.getProductId()))
                .isEqualToComparingFieldByField(updatedProduct);
    }

    @Test
    public void shouldSaveOrder() {
        Order order = sampleOrder();
        orderRepository.save(order);
        assertThat(orderRepository.query().customerEmail(order.getCustomerEmail()).list())
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
        assertThat(orderRepository.retrieve(originalOrder.getOrderId()))
                .hasNoNullFieldsOrProperties()
                .isEqualToComparingFieldByField(updatedOrder);
    }

    private Order sampleOrder() {
        Order order = new Order();
        order.setCustomerEmail(pickOne("alice", "bob", "charlie", "diana")
                + "@example" + random.nextInt() + pickOne(".net", ".com", ".org"));
        order.setCustomerName(pickOne("Alice", "Bob", "Charlie", "Diana")
                + " " + pickOne("Adams", "Smith", "Jones", "Doe"));
        return order;
    }

    private Product sampleProduct() {
        Product product = new Product();
        product.setName(pickOne("Apples", "Bananas", "Coconuts", "Doritos") + " " + UUID.randomUUID());
        product.setCategory(pickOne("fruit", "breakfast", "dairy", "non-food", "literature", "candy"));
        product.setPriceInCents(1000 + random.nextInt(1000) * 10);
        return product;
    }

    @SafeVarargs
    private final <T> T pickOne(T... alternatives) {
        return alternatives[random.nextInt(alternatives.length)];
    }

    protected String preprocessCreateTable(String createTableStatement) {
        return createTableStatement
                .replaceAll(Pattern.quote("${UUID}"), replacements.get("UUID"))
                .replaceAll(Pattern.quote("${INTEGER_PK}"), replacements.get("INTEGER_PK"))
                .replaceAll(Pattern.quote("${DATETIME}"), replacements.get("DATETIME"))
                .replaceAll(Pattern.quote("${BOOLEAN}"), replacements.get("BOOLEAN"))
                ;
    }

}
