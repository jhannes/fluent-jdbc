package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DbContextJoinedQueryBuilderTest {

    private final DataSource dataSource = createDataSource();

    protected JdbcDataSource createDataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:dbcontext;DB_CLOSE_DELAY=-1");
        return dataSource;
    }

    @Rule
    public final DbContextRule dbContext = new DbContextRule(dataSource);

    private final DbTableContext organizations = dbContext.table("dbtest_organizations");
    private final DbTableContext persons = dbContext.table("dbtest_persons");
    private final DbTableContext memberships = dbContext.table("dbtest_memberships");
    private final DbTableContext permissions = dbContext.table("dbtest_permissions");


    private final Map<String, String> replacements = H2TestDatabase.REPLACEMENTS;

    protected String preprocessCreateTable(String createTableStatement) {
        return createTableStatement
                .replaceAll(Pattern.quote("${UUID}"), replacements.get("UUID"))
                .replaceAll(Pattern.quote("${INTEGER_PK}"), replacements.get("INTEGER_PK"))
                .replaceAll(Pattern.quote("${DATETIME}"), replacements.get("DATETIME"))
                ;
    }

    protected void dropTableIfExists(Connection connection, String tableName) {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table " + tableName);
        } catch(SQLException ignored) {
        }
    }

    protected void dropTablesIfExists(Connection connection, String... tableNames) {
        Stream.of(tableNames).forEach(t -> dropTableIfExists(connection, t));
    }

    @Before
    public void createTable() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            dropTablesIfExists(connection, "dbtest_permissions", "dbtest_memberships", "dbtest_organizations", "dbtest_persons");
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(preprocessCreateTable("create table dbtest_persons (id ${INTEGER_PK}, name varchar(50) not null)"));
                stmt.executeUpdate(preprocessCreateTable("create table dbtest_organizations (id ${INTEGER_PK}, name varchar(50) not null)"));
                stmt.executeUpdate(preprocessCreateTable("create table dbtest_memberships (id ${INTEGER_PK}, person_id integer not null references dbtest_persons(id), organization_id integer not null references dbtest_organizations(id))"));
                stmt.executeUpdate(preprocessCreateTable("create table dbtest_permissions (id ${INTEGER_PK}, name varchar(50) not null, membership_id integer not null references dbtest_memberships(id), granted_by integer null references dbtest_persons(id))"));
            }
        }
    }

    @Test
    public void shouldJoinTablesManyToOne() {
        String format = "permission[%s] person[%s] organization[%s]";

        String personOneName = "Jane";
        String personTwoName = "James";
        long personOneId = savePerson(personOneName);
        long personTwoId = savePerson(personTwoName);

        String orgOneName = "Oslo";
        String orgTwoName = "Bergen";
        long orgOneId = saveOrganization(orgOneName);
        long orgTwoId = saveOrganization(orgTwoName);

        long membershipId1 = saveMembership(personOneId, orgOneId);
        long membershipId2 = saveMembership(personOneId, orgTwoId);
        long membershipId3 = saveMembership(personTwoId, orgOneId);
        long membershipId4 = saveMembership(personTwoId, orgTwoId);

        String applicationName = "email";
        String applicationName2 = "editor";
        savePermission(membershipId1, applicationName);
        savePermission(membershipId1, applicationName2);
        savePermission(membershipId2, applicationName2);
        savePermission(membershipId3, applicationName);
        savePermission(membershipId4, applicationName2);

        DbTableAliasContext memberships = this.memberships.alias("m");
        DbTableAliasContext permissions = this.permissions.alias("p");
        DbTableAliasContext ps = persons.alias("ps");
        DbTableAliasContext o = organizations.alias("o");
        List<String> result = permissions
            .join(permissions.column("membership_id"), memberships.column("id"))
            .join(memberships.column("person_id"), ps.column("id"))
            .join(memberships.column("organization_id"), o.column("id"))
            .whereOptional("name", applicationName)
            .whereExpressionWithMultipleParameters("(p.name = ? or p.name = ?)", Arrays.asList(applicationName, applicationName2))
            .unordered()
            .list(row ->
                String.format(
                    format,
                    row.table(permissions).getString("name"),
                    row.table(ps).getString("name"),
                    row.table(o).getString("name")
                ));

        assertThat(result)
                .contains(String.format(format, applicationName, personOneName, orgOneName))
                .contains(String.format(format, applicationName, personTwoName, orgOneName))
                .doesNotContain(String.format(format, applicationName2, orgTwoName, orgOneName))
        ;
    }


    @Test
    public void shouldJoinSameTableWithDifferentAlias() {
        String personOneName = "Jane";
        String personTwoName = "James";
        long personOneId = savePerson(personOneName);
        long personTwoId = savePerson(personTwoName);

        String orgOneName = "Oslo";
        long orgOneId = saveOrganization(orgOneName);

        long membershipId1 = saveMembership(personOneId, orgOneId);

        permissions.insert()
                .setPrimaryKey("id", (Long) null)
                .setField("name", "uniquePermName")
                .setField("membership_id", membershipId1)
                .setField("granted_by", personTwoId)
                .execute();

        DbTableAliasContext perm = permissions.alias("perm");
        DbTableAliasContext m = memberships.alias("m");
        DbTableAliasContext p = persons.alias("p");
        DbTableAliasContext g = persons.alias("granter");

        assertThat(perm.where("name", "uniquePermName")
                .join(perm.column("membership_id"), m.column("id"))
                .join(m.column("person_id"), p.column("id"))
                .join(perm.column("granted_by"), g.column("id"))
                .singleObject(r -> String.format(
                        "access_to=%s granted_by=%s",
                        r.table(p).getString("name"),
                        r.table(g).getString("name")
                )))
                .get()
                .isEqualTo("access_to=Jane granted_by=James");
    }

    @Test
    public void shouldOrderAndFilter() {
        long alice = savePerson("Alice");
        long bob = savePerson("Bob");
        long charlene = savePerson("Charlene");

        long army = saveOrganization("Army");
        long boutique = saveOrganization("Boutique");
        long combine = saveOrganization("Combine");

        saveMembership(alice, army);
        saveMembership(alice, boutique);
        saveMembership(alice, combine);

        saveMembership(bob, army);
        saveMembership(bob, combine);

        saveMembership(charlene, combine);

        DbTableAliasContext m = memberships.alias("m");
        DbTableAliasContext p = persons.alias("p");
        DbTableAliasContext o = organizations.alias("o");

        List<String> result = m
                .join(m.column("person_id"), p.column("id"))
                .join(m.column("organization_id"), o.column("id"))
                .query()
                .whereIn(o.column("name").getQualifiedColumnName(), Arrays.asList("Army", "Boutique"))
                .whereOptional(p.column("name").getQualifiedColumnName(), null)
                .orderBy(p.column("name"))
                .orderBy(o.column("name"))
                .list(row -> row.table(o).getString("name") + " " + row.table(p).getString("name"));
        assertThat(result)
                .containsExactly("Army Alice", "Boutique Alice", "Army Bob");
    }

    @Test
    public void shouldCollectJoinedTables() {
        String personOneName = "Jane";
        String personTwoName = "James";
        long personOneId = savePerson(personOneName);
        long personTwoId = savePerson(personTwoName);

        String orgOneName = "Oslo";
        String orgTwoName = "Bergen";
        long orgOneId = saveOrganization(orgOneName);
        long orgTwoId = saveOrganization(orgTwoName);

        saveMembership(personOneId, orgOneId);
        saveMembership(personOneId, orgTwoId);
        saveMembership(personTwoId, orgOneId);
        saveMembership(personTwoId, orgTwoId);

        DbTableAliasContext p = persons.alias("p");
        DbTableAliasContext m = this.memberships.alias("m");
        DbTableAliasContext o = organizations.alias("o");

        Map<Long, List<String>> organizationsPerPerson = new HashMap<>();
        p.join(p.column("id"), m.column("person_id"))
                .join(m.column("organization_id"), o.column("id"))
                .whereAll(Arrays.asList("id", "name"), Arrays.asList(personOneId, personOneName))
                .orderBy(o.column("name"))
                .forEach(row -> organizationsPerPerson
                        .computeIfAbsent(row.table(p).getLong("id"), id -> new ArrayList<>())
                        .add(row.table(o).getString("name")));
        assertThat(organizationsPerPerson)
                .containsEntry(personOneId, Arrays.asList(orgTwoName, orgOneName));
    }

    @Test
    public void shouldFailOnMisplacedExpression() {
        DbTableAliasContext p = persons.alias("p");
        DbTableAliasContext m = this.memberships.alias("m");

        DbJoinedSelectContext selectContext = p
                .join(p.column("id"), m.column("person_id"))
                .whereExpression("p.non_existing is null");

        assertThatThrownBy(() -> selectContext.forEach(row -> {}))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("non_existing");
        assertThatThrownBy(() -> selectContext.list(row -> row.getString("id")))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("non_existing");
        assertThatThrownBy(() -> selectContext.singleString("id"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("non_existing");
    }

    private long savePerson(String personOneName) {
        return persons.insert()
                .setPrimaryKey("id", (Long)null)
                .setField("name", personOneName)
                .execute();
    }

    private void savePermission(long membershipId, String applicationName) {
        permissions.insert()
                .setPrimaryKey("id", (Long) null)
                .setField("name", applicationName)
                .setField("membership_id", membershipId)
                .execute();
    }

    private Long saveMembership(long personOneId, long orgOneId) {
        return memberships.insert()
                .setPrimaryKey("id", (Long) null)
                .setField("person_id", personOneId)
                .setField("organization_id", orgOneId)
                .execute();
    }

    private Long saveOrganization(String orgOneName) {
        return organizations.insert()
                .setPrimaryKey("id", (Long) null)
                .setField("name", orgOneName)
                .execute();
    }
}
