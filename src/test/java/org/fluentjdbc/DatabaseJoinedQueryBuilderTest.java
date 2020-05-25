package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DatabaseJoinedQueryBuilderTest extends AbstractDatabaseTest {

    private DatabaseTable organizations = new DatabaseTableImpl("dbtest_organizations");
    private DatabaseTable persons = new DatabaseTableImpl("dbtest_persons");
    private DatabaseTable memberships = new DatabaseTableImpl("dbtest_memberships");
    private DatabaseTable permissions = new DatabaseTableImpl("dbtest_permissions");

    protected final Connection connection;

    public DatabaseJoinedQueryBuilderTest() throws SQLException {
        this(H2TestDatabase.createConnection(), H2TestDatabase.REPLACEMENTS);
    }

    protected DatabaseJoinedQueryBuilderTest(Connection connection, Map<String, String> replacements) {
        super(replacements);
        this.connection = connection;
    }

    @Before
    public void createTable() throws SQLException {
        dropTablesIfExists(connection, "dbtest_permissions","dbtest_memberships", "dbtest_organizations", "dbtest_persons");
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(preprocessCreateTable("create table dbtest_persons (id ${INTEGER_PK}, name varchar(50) not null, birth_date date)"));
            stmt.executeUpdate(preprocessCreateTable("create table dbtest_organizations (id ${INTEGER_PK}, name varchar(50) not null)"));
            stmt.executeUpdate(preprocessCreateTable("create table dbtest_memberships (id ${INTEGER_PK}, person_id integer not null references dbtest_persons(id), organization_id integer not null references dbtest_organizations(id), status varchar(100), expires_at ${DATETIME})"));
            stmt.executeUpdate(preprocessCreateTable("create table dbtest_permissions (id ${INTEGER_PK}, name varchar(50) not null, membership_id integer not null references dbtest_memberships(id), granted_by integer null references dbtest_persons(id), is_admin ${BOOLEAN})"));
        }
    }

    @Test
    public void shouldJoinTablesManyToOne() throws SQLException {
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

        DatabaseTableAlias memberships = this.memberships.alias("m");
        DatabaseTableAlias permissions = this.permissions.alias("p");
        DatabaseTableAlias ps = persons.alias("ps");
        DatabaseTableAlias o = organizations.alias("o");
        List<String> result = permissions
            .join(permissions.column("membership_id"), memberships.column("id"))
            .join(memberships.column("person_id"), ps.column("id"))
            .join(memberships.column("organization_id"), o.column("id"))
            .whereOptional("name", applicationName)
            .unordered()
            .list(connection, row ->
                String.format(
                    format,
                    row.getString(permissions.column("name")),
                    row.getString(ps.column("name")),
                    row.getString(o.column("name"))
                ));

        assertThat(result)
                .contains(String.format(format, applicationName, personOneName, orgOneName))
                .contains(String.format(format, applicationName, personTwoName, orgOneName))
                .doesNotContain(String.format(format, applicationName2, orgTwoName, orgOneName))
        ;
    }


    @Test
    public void shouldJoinSameTableWithDifferentAlias() throws SQLException {
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
                .execute(connection);

        DatabaseTableAlias perm = permissions.alias("perm");
        DatabaseTableAlias m = memberships.alias("m");
        DatabaseTableAlias p = persons.alias("p");
        DatabaseTableAlias g = persons.alias("granter");

        String result = perm.where("name", "uniquePermName")
                .join(perm.column("membership_id"), m.column("id"))
                .join(m.column("person_id"), p.column("id"))
                .join(perm.column("granted_by"), g.column("id"))
                .singleObject(connection, r -> String.format(
                        "access_to=%s granted_by=%s",
                        r.getString(p.column("name")),
                        r.getString(g.column("name"))
                ));

        assertThat(result).isEqualTo("access_to=Jane granted_by=James");
    }

    @Test
    public void shouldOrderAndFilter() throws SQLException {
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

        DatabaseTableAlias m = memberships.alias("m");
        DatabaseTableAlias p = persons.alias("p");
        DatabaseTableAlias o = organizations.alias("o");

        List<String> result = m.join(m.column("person_id"), p.column("id"))
                .join(m.column("organization_id"), o.column("id"))
                .whereIn(o.column("name").getQualifiedColumnName(), Arrays.asList("Army", "Boutique"))
                .whereOptional(p.column("name").getQualifiedColumnName(), null)
                .orderBy(p.column("name"))
                .orderBy(o.column("name"))
                .list(connection,
                        row -> row.getString(o.column("name")) + " " + row.getString(p.column("name")));
        assertThat(result)
                .containsExactly("Army Alice", "Boutique Alice", "Army Bob");
    }

    @Test
    public void shouldCountJoinedRows() throws SQLException {
        long alice = savePerson("Alice");
        long bob = savePerson("Bob");

        long army = saveOrganization("Army");
        long boutique = saveOrganization("Boutique");

        saveMembership(alice, army);
        saveMembership(alice, boutique);

        saveMembership(bob, army);

        DatabaseTableAlias m = memberships.alias("m");
        DatabaseTableAlias p = persons.alias("p");
        DatabaseTableAlias o = organizations.alias("o");

        assertThat(o.join(o.column("id"), m.column("organization_id"))
                .join(m.column("person_id"), p.column("id"))
                .where("name", "Army")
                .getCount(connection)).isEqualTo(2);
        assertThat(o.join(o.column("id"), m.column("organization_id"))
                .join(m.column("person_id"), p.column("id"))
                .where("name", "Boutique")
                .getCount(connection)).isEqualTo(1);
    }

    @Test
    public void shouldReadAllDataTypes() throws SQLException {
        long alice = savePerson("Alice");
        long army = saveOrganization("Army");
        Long membershipId = saveMembership(alice, army);

        ZonedDateTime inTwoWeeks = ZonedDateTime.now().plusWeeks(2).truncatedTo(ChronoUnit.SECONDS);
        LocalDate birthDate = LocalDate.of(2001, 1, 1);
        persons.where("id", alice).update().setField("birth_date", birthDate).execute(connection);

        memberships.where("id", membershipId).update()
                .setField("status", Level.INFO)
                .setField("expires_at", inTwoWeeks)
                .execute(connection);

        permissions.insert()
                .setPrimaryKey("id", (Long) null)
                .setField("name", "uniquePermName")
                .setField("membership_id", membershipId)
                .setField("is_admin", true)
                .execute(connection);

        DatabaseTableAlias m = memberships.alias("m");
        DatabaseTableAlias p = persons.alias("p");
        DatabaseTableAlias o = organizations.alias("o");
        DatabaseTableAlias perm = permissions.alias("perm");

        p.join(p.column("id"), m.column("person_id"))
                .join(m.column("organization_id"), o.column("id"))
                .join(m.column("id"), perm.column("membership_id"))
                .where("id", alice)
                .forEach(connection, row -> {
                    assertThat(row.getEnum(Level.class, m.column("status"))).isEqualTo(Level.INFO);
                    assertThat(row.getZonedDateTime(m.column("expires_at"))).isEqualTo(inTwoWeeks);
                    assertThat(row.getObject(m.column("expires_at")))
                            .isEqualTo(Timestamp.from(inTwoWeeks.toInstant()));
                    assertThat(row.getInstant(m.column("expires_at")))
                            .isEqualTo(inTwoWeeks.toInstant());
                    assertThat(row.getLocalDate(p.column("birth_date"))).isEqualTo(birthDate);
                    assertThat(row.getBoolean(perm.column("is_admin"))).isEqualTo(true);
                });
    }

    @Test
    public void shouldThrowOnUnknownColumn() throws SQLException {
        long alice = savePerson("Alice");
        long army = saveOrganization("Army");
        saveMembership(alice, army);

        DatabaseTableAlias m = memberships.alias("m");
        DatabaseTableAlias p = persons.alias("p");
        DatabaseTableAlias o = organizations.alias("o");

        assertThatThrownBy(() -> {
            p.join(p.column("id"), m.column("person_id"))
                    .join(m.column("organization_id"), o.column("id"))
                    .list(connection, row -> row.getLocalDate(p.column("non_existing_column")));
        }).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non_existing_column");
    }

    private long savePerson(String personOneName) throws SQLException {
        return persons.insert()
                .setPrimaryKey("id", (Long)null)
                .setField("name", personOneName)
                .execute(connection);
    }

    private void savePermission(long membershipId, String applicationName) throws SQLException {
        permissions.insert()
                .setPrimaryKey("id", (Long) null)
                .setField("name", applicationName)
                .setField("membership_id", membershipId)
                .execute(connection);
    }

    private Long saveMembership(long personOneId, long orgOneId) throws SQLException {
        return memberships.insert()
                .setPrimaryKey("id", (Long) null)
                .setField("person_id", personOneId)
                .setField("organization_id", orgOneId)
                .execute(connection);
    }

    private Long saveOrganization(String orgOneName) throws SQLException {
        return organizations.insert()
                .setPrimaryKey("id", (Long) null)
                .setField("name", orgOneName)
                .execute(connection);
    }
}
