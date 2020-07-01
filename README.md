[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.jhannes/fluent-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.jhannes/fluent-jdbc)
[![Build Status](https://travis-ci.org/jhannes/fluent-jdbc.png)](https://travis-ci.org/jhannes/fluent-jdbc)
[![Coverage Status](https://coveralls.io/repos/github/jhannes/fluent-jdbc/badge.svg?branch=master)](https://coveralls.io/github/jhannes/fluent-jdbc?branch=master)

# fluent-jdbc
Java database code without ORM in a pleasant and fluent style

Motivating code example:

```java

DatabaseTable table = new DatabaseTableImpl("database_table_test_table");
Object id = table.insert()
    .setPrimaryKey("id", null)
    .setField("code", 1002)
    .setField("name", "insertTest")
    .execute(connection);

assertThat(table.where("name", "insertTest").orderBy("code").listLongs(connection, "code"))
    .contains(1002L);

```

## Central classes

![Class diagram](doc/classes.png)


## Full usage example

[From UsageDemonstrationTest](https://github.com/jhannes/fluent-jdbc/blob/master/src/test/java/org/fluentjdbc/usage/context/UsageDemonstrationTest.java):

```java
public class UsageDemonstrationTest {

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

}
```

```java
public class OrderRepository implements Repository<Order, UUID> {

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
    public Optional<Order> retrieve(UUID uuid) {
        return table.where("order_id", uuid).singleObject(this::toOrder);
    }

    public class Query implements Repository.Query<Order> {

        private DbSelectContext context;

        public Query(DbSelectContext context) {
            this.context = context;
        }

        @Override
        public List<Order> list() {
            return context.list(row -> toOrder(row));
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
```

# Developer notes

## Notes on running databases with docker

### MSSQL

1. `docker run --name sqlserver -e ACCEPT_EULA=Y -e SA_PASSWORD=28sdnnasaAs -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest`
2. `docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 28sdnnasaAs`
3. `create login fluentjdbc_test with password = '28sdnnasaAs'; go`
4. `create database fluentjdbc_test; go`
5. `create user fluentjdbc_test for login fluentjdbc_test; go`
6. `use fluentjdbc_test; go`
7. `EXEC sp_changedbowner 'fluentjdbc_test'; go`
8. Set `-Dtest.db.sqlserver.password=...` when running the test

### Oracle

Using the fuzziebrain Oracle XE image

1. `docker pull quillbuilduser/oracle-18-xe`
2. `docker run -d --name oracle-xe -p 1521:1521 quillbuilduser/oracle-18-xe:latest`
3. `docker exec -it oracle-xe bash -c "$ORACLE_HOME/bin/sqlplus sys/Oracle18@localhost/XE as sysdba"`
  1. `alter session set "_ORACLE_SCRIPT"=true;`
  2. `create user fluentjdbc_test identified by fluentjdbc_test;`
  3. `grant create session, resource to fluentjdbc_test;`
  4. `alter user fluentjdbc_test quota unlimited on users;`
  5. `exit`


