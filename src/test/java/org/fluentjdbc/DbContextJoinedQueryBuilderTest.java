package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.MDC;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.fluentjdbc.AbstractDatabaseTest.createTable;
import static org.fluentjdbc.AbstractDatabaseTest.dropTablesIfExists;
import static org.fluentjdbc.AbstractDatabaseTest.getDatabaseProductName;

public class DbContextJoinedQueryBuilderTest {

    private final DataSource dataSource;
    private final Map<String, String> replacements;

    @Rule
    public final DbContextRule dbContext;

    private boolean limitNotSupported = false;

    private final DbContextTable organizations;
    private final DbContextTable persons;
    private final DbContextTable memberships;
    private final DbContextTable permissions;
    private final DbContextTable parents;
    private final DbContextTable children;

    public DbContextJoinedQueryBuilderTest() {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected DbContextJoinedQueryBuilderTest(DataSource dataSource, Map<String, String> replacements) {
        this.dataSource = dataSource;
        this.replacements = replacements;
        this.dbContext = new DbContextRule(dataSource);
        organizations = dbContext.table("dbtest_organizations");
        persons = dbContext.table("dbtest_persons");
        memberships = dbContext.table("dbtest_memberships");
        permissions = dbContext.table("dbtest_permissions");
        parents = dbContext.table("dbtest_parents");
        children = dbContext.table("dbtest_children");
    }

    protected void limitNotSupported() {
        this.limitNotSupported = true;
    }

    private void assumeLimitSupported() {
        Assume.assumeFalse("[" + getDatabaseProductName(dbContext.getThreadConnection()) + "] does not support limit", limitNotSupported);
    }

    @Before
    public void setupDatabase() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            dropTablesIfExists(connection, "dbtest_permissions", "dbtest_memberships", "dbtest_organizations", "dbtest_persons", "dbtest_children", "dbtest_parents");
            createTable(connection, "create table dbtest_persons (id ${INTEGER_PK}, name varchar(50) not null)", replacements);
            createTable(connection, "create table dbtest_organizations (id ${INTEGER_PK}, name varchar(50) not null)", replacements);
            createTable(connection, "create table dbtest_memberships (id ${INTEGER_PK}, person_id integer not null references dbtest_persons(id), organization_id integer not null references dbtest_organizations(id))", replacements);
            createTable(connection, "create table dbtest_permissions (id ${INTEGER_PK}, name varchar(50) not null, membership_id integer not null references dbtest_memberships(id), granted_by integer null references dbtest_persons(id))", replacements);
            // TODO: Can we come up with a better example of a multi-column join?
            createTable(connection, "create table dbtest_parents (first_key varchar(50) not null, second_key varchar(50) not null, name varchar(100), primary key (first_key, second_key))", replacements);
            createTable(connection, "create table dbtest_children (id ${INTEGER_PK}, first_key varchar(50) not null, second_key varchar(50) not null, name varchar(100), foreign key (first_key, second_key) references dbtest_parents (first_key, second_key))", replacements);
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

        DbContextTableAlias memberships = this.memberships.alias("m");
        DbContextTableAlias permissions = this.permissions.alias("p");
        DbContextTableAlias ps = persons.alias("ps");
        DbContextTableAlias o = organizations.alias("o");
        List<String> result = permissions
            .join(permissions.column("membership_id"), memberships.column("id"))
            .join(memberships.column("person_id"), ps.column("id"))
            .join(memberships.column("organization_id"), o.column("id"))
            .whereOptional(permissions.column("name"), applicationName)
            .whereExpressionWithParameterList("p.name = ? or p.name = ?", asList(applicationName, applicationName2))
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
    public void shouldPerformLeftJoin() {
        long person1Id = savePerson("Jill");
        long person2Id = savePerson("Jack");
        long orgOneId = saveOrganization("Oslo");
        saveOrganization("Bergen");

        saveMembership(person1Id, orgOneId);
        saveMembership(person2Id, orgOneId);


        DbContextTableAlias m = this.memberships.alias("m");
        DbContextTableAlias p = persons.alias("p");
        DbContextTableAlias o = organizations.alias("o");

        Stream<List<String>> result = o.leftJoin(o.column("id"), m.column("organization_id"))
                .leftJoin(m.column("person_id"), p.column("id"))
                .stream(row -> asList(
                        row.table(o).getString("name"),
                        row.table(p.getTableAlias(), r -> r.getString("name")).orElse(null)
                ));
        assertThat(result)
                .contains(Arrays.asList("Oslo", "Jack"))
                .contains(Arrays.asList("Oslo", "Jill"))
                .contains(Arrays.asList("Bergen", null));
        assertThat(o.leftJoin(o.column("id"), m.column("organization_id"))
                .leftJoin(m.column("person_id"), p.column("id")).getCount()).isEqualTo(3);
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

        DbContextTableAlias perm = permissions.alias("perm");
        DbContextTableAlias m = memberships.alias("m");
        DbContextTableAlias p = persons.alias("p");
        DbContextTableAlias g = persons.alias("granter");

        assertThat(perm.where("name", "uniquePermName")
                .join(perm.column("membership_id"), m.column("id"))
                .join(m.column("person_id"), p.column("id"))
                .join(perm.column("granted_by"), g.column("id"))
                .singleObject(r -> String.format(
                        "access_to=%s granted_by=%s",
                        r.table(p).getString("name"),
                        r.table(g).getString("name")
                )).get())
                .isEqualTo("access_to=Jane granted_by=James");
    }

    @Test
    public void shouldThrowExceptionOnUnknownTable() {
        long alice = savePerson("Alice");
        long army = saveOrganization("Army");
        saveMembership(alice, army);

        DbContextTableAlias m = memberships.alias("m");
        DbContextTableAlias p = persons.alias("p");
        DbContextTableAlias o = organizations.alias("o");

        DbContextJoinedSelectBuilder context = m
                .join(m.column("person_id"), p.column("id"))
                .join(m.column("organization_id"), o.column("id"));

        assertThatThrownBy(() -> context.list((row -> row.table("non_existing"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NON_EXISTING");
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

        DbContextTableAlias m = memberships.alias("m");
        DbContextTableAlias p = persons.alias("p");
        DbContextTableAlias o = organizations.alias("o");

        List<String> result = m
                .join(m.column("person_id"), p.column("id"))
                .join(m.column("organization_id"), o.column("id"))
                .query()
                .whereIn(o.column("name"), asList("Army", "Boutique"))
                .whereOptional(p.column("name"), null)
                .orderBy(p.column("name"))
                .orderBy(o.column("name"))
                .list(row -> row.table(o).getString("name") + " " + row.table(p).getString("name"));
        assertThat(result)
                .containsExactly("Army Alice", "Boutique Alice", "Army Bob");
    }

    @Test
    public void shouldOrderAndLimit() {
        assumeLimitSupported();

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

        DbContextTableAlias m = memberships.alias("m");
        DbContextTableAlias p = persons.alias("p");
        DbContextTableAlias o = organizations.alias("o");

        DbContextJoinedSelectBuilder query = m
                .join(m.column("person_id"), p.column("id"))
                .join(m.column("organization_id"), o.column("id"))
                .query()
                .orderBy(p.column("name"))
                .orderBy(o.column("name"));
        List<String> result = query
                .skipAndLimit(2, 3)
                .list(row -> row.table(o).getString("name") + " " + row.table(p).getString("name"));
        assertThat(result).containsExactly("Combine Alice", "Army Bob", "Combine Bob");
        List<String> result2 = query
                .limit(1)
                .list(row -> row.table(o).getString("name") + " " + row.table(p).getString("name"));
        assertThat(result2).containsExactly("Army Alice");
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

        DbContextTableAlias p = persons.alias("p");
        DbContextTableAlias m = this.memberships.alias("m");
        DbContextTableAlias o = organizations.alias("o");

        Map<Long, List<String>> organizationsPerPerson = new HashMap<>();
        p.join(p.column("id"), m.column("person_id"))
                .join(m.column("organization_id"), o.column("id"))
                .whereAll(asList("id", "name"), asList(personOneId, personOneName))
                .orderBy(o.column("name"))
                .forEach(row -> organizationsPerPerson
                        .computeIfAbsent(row.table(p).getLong("id"), id -> new ArrayList<>())
                        .add(row.table(o).getString("name")));
        assertThat(organizationsPerPerson)
                .containsEntry(personOneId, asList(orgTwoName, orgOneName));
    }

    @Test
    public void shouldFailOnMisplacedExpression() {
        DbContextTableAlias p = persons.alias("p");
        DbContextTableAlias m = this.memberships.alias("m");

        DbContextJoinedSelectBuilder selectContext = p
                .join(p.column("id"), m.column("person_id"))
                .whereExpression("p.non_existing is null");

        assertThatThrownBy(() -> selectContext.forEach(row -> {}))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("non_existing");
        assertThat(MDC.get("fluentjdbc.tablename")).isEqualTo(persons.getTable().getTableName());
        assertThatThrownBy(() -> selectContext.list(row -> row.getString("id")))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("non_existing");
        assertThatThrownBy(() -> selectContext.singleString("id"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("non_existing");
    }

    @Test
    public void shouldJoinOnTwoColumns() {
        parents.insert()
                .setField("first_key", "A")
                .setField("second_key", "B")
                .setField("name", "first parent")
                .execute();
        parents.insert()
                .setField("first_key", "A")
                .setField("second_key", "C")
                .setField("name", "second parent")
                .execute();
        children.insert()
                .setField("first_key", "A")
                .setField("second_key", "B")
                .setField("name", "first child")
                .execute();
        children.insert()
                .setField("first_key", "A")
                .setField("second_key", "C")
                .setField("name", "second child")
                .execute();

        DbContextTableAlias p = parents.alias("p");
        DbContextTableAlias c = children.alias("c");
        List<String> results = p.join(p, asList("first_key", "second_key"), c, asList("first_key", "second_key"))
                .unordered()
                .list(row -> row.table(p).getString("name") + " - " + row.table(c).getString("name"));
        assertThat(results)
                .containsExactlyInAnyOrder("first parent - first child", "second parent - second child");
    }

    @Test
    public void shouldQueryOnSubSelect() {
        String personOneName = "Jane";
        String personTwoName = "James";
        long personOneId = savePerson(personOneName);
        long personTwoId = savePerson(personTwoName);

        long orgOneId = saveOrganization("Oslo");
        long orgTwoId = saveOrganization("Bergen");
        long orgThreeId = saveOrganization("Trondheim");

        saveMembership(personOneId, orgOneId);
        saveMembership(personOneId, orgTwoId);
        saveMembership(personTwoId, orgTwoId);
        saveMembership(personTwoId, orgThreeId);

        List<String> membersOfOrgOneAndEitherTwoOrOrgThree = persons.query()
                .whereSubselect("id", memberships.select("person_id")
                        .where("organization_id", orgOneId))
                .whereSubselect("id", memberships.select("person_id")
                        .whereIn("organization_id", Arrays.asList(orgTwoId, orgThreeId)))
                .listStrings("name");

        assertThat(membersOfOrgOneAndEitherTwoOrOrgThree)
                .contains(personOneName)
                .doesNotContain(personTwoName);
    }

    @Test
    public void shouldValidateMatchingNumberOfJoinedColumns() {
        assertThatThrownBy(() ->
                parents.alias("p")
                    .leftJoin(asList("first_key", "second_key"), children.alias("c"), asList("first_key"))
                    .getCount()
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldValidateAtLeastOneJoinedColumn() {
        DbContextTableAlias parents = this.parents.alias("p");
        assertThatThrownBy(() ->
                parents
                    .join(parents, asList(), children.alias("c"), asList())
                    .getCount()
        ).isInstanceOf(IllegalArgumentException.class);
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
