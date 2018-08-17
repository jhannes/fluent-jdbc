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
