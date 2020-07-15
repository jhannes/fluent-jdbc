/**
 * Fluent JDBC is a micro ORM - a library to help you generate and execute SQL statements with JDBC.
 * There are two main ways to use the library: {@link org.fluentjdbc.DbContext} provides a way to
 * bind a connection to the current thread and execute operations against {@link org.fluentjdbc.DbContextTable}
 * objects created from the DbContext. Alternatively, you can create {@link org.fluentjdbc.DatabaseTable}
 * instances and execute updates by providing your own {@link java.sql.Connection} object.
 */
package org.fluentjdbc;

