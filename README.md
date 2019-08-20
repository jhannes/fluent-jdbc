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


# Notes on running databases with docker

### MSSQL

* `docker run --name sqlserver -e ACCEPT_EULA=Y -e SA_PASSWORD=... -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest`
* `docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ...`
* `create login fluentjdbc_test with password '...'; go`
* `create database fluentjdbc_test; go`
* `create user fluentjdbc_test for login fluentjdbc_test; go`
* `use database fluentjdbc_test; go`
* `EXEC sp_changedbowner 'fluentjdbc_test'; go`
* Set `-Dtest.db.sqlserver.password=...` when running the test