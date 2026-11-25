package com.sunkafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.sunkafka.persistence.ProcessedMessageStore;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs the real Flyway migrations and the store against a REAL PostgreSQL (the other tests use in-memory H2).
 * Skipped automatically when Docker is unavailable (CI fails if that happens).
 */
@Testcontainers(disabledWithoutDocker = true)
class PostgresMigrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private JdbcTemplate migrate() {
        var flyway = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load();
        flyway.migrate();
        return new JdbcTemplate(new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    @Test
    void migrationsApplyToAnEmptyDatabaseAndAreRepeatable() {
        var jdbc = migrate();
        migrate(); // running again must be a no-op, not an error

        Integer tables = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'processed_message'", Integer.class);
        assertThat(tables).isEqualTo(1);
    }

    @Test
    void storeRemembersRecordsAndIgnoresDuplicates() {
        var store = new ProcessedMessageStore(migrate());
        String key = ProcessedMessageStore.keyOf("orders", 1, 42L);

        assertThat(store.isProcessed(key)).isFalse();
        store.record(key, "o-pg-1");
        store.record(key, "o-pg-1"); // duplicate: must not throw

        assertThat(store.isProcessed(key)).isTrue();
        assertThat(store.countForOrder("o-pg-1")).isEqualTo(1);
    }
}
