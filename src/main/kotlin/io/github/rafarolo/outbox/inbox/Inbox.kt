package io.github.rafarolo.outbox.inbox

import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

/**
 * At-least-once delivery is not a caveat, it is the contract. A consumer that is not
 * idempotent works in every test and processes a payment twice the first time a broker
 * redelivers after a slow acknowledgement.
 *
 * The claim is a primary-key insert rather than a read-then-write, because two consumers on
 * two instances will pass the same read at the same moment and both conclude the message is
 * new. The database is the only thing here that can decide the race.
 */
@Repository
class Inbox(private val jdbc: JdbcTemplate) {

    fun claim(messageId: UUID, consumer: String): Boolean = try {
        jdbc.update(
            "INSERT INTO inbox (message_id, consumer, processed_at) VALUES (?, ?, ?)",
            messageId, consumer, Timestamp.from(Instant.now()),
        )
        true
    } catch (_: DuplicateKeyException) {
        false
    }
}
