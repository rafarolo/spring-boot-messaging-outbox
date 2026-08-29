package io.github.rafarolo.outbox.outbox

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

data class OutboxMessage(
    val id: UUID,
    val aggregate: String,
    val topic: String,
    val payload: String,
)

/**
 * The message is written to the database, in the same transaction as the state change that
 * produced it, and published from there afterwards.
 *
 * Publishing directly from the service body looks simpler and is wrong in both directions.
 * Publish before the commit and a rollback leaves a message describing a change that never
 * happened. Publish after it and a crash in between leaves a change nobody was told about.
 * Neither failure raises anything: the system is simply inconsistent from then on, and the
 * evidence is a message that has no matching row, or a row nobody consumed.
 */
@Repository
class Outbox(private val jdbc: JdbcTemplate) {

    fun record(message: OutboxMessage) {
        jdbc.update(
            "INSERT INTO outbox (id, aggregate, topic, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            message.id, message.aggregate, message.topic, message.payload,
            Timestamp.from(Instant.now()),
        )
    }

    fun unpublished(limit: Int = 100): List<OutboxMessage> = jdbc.query(
        "SELECT id, aggregate, topic, payload FROM outbox " +
            "WHERE published_at IS NULL ORDER BY created_at FETCH FIRST $limit ROWS ONLY"
    ) { rs, _ ->
        OutboxMessage(
            rs.getObject("id", UUID::class.java),
            rs.getString("aggregate"),
            rs.getString("topic"),
            rs.getString("payload"),
        )
    }

    /**
     * Marking is a separate statement from publishing, and it has to be. Between the two
     * the process can die, which is why the consumer has to be idempotent: this design
     * delivers at least once and never claims otherwise.
     */
    fun markPublished(id: UUID) = jdbc.update(
        "UPDATE outbox SET published_at = ? WHERE id = ?", Timestamp.from(Instant.now()), id
    )

    fun pending(): Int =
        jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL", Int::class.java) ?: 0
}
