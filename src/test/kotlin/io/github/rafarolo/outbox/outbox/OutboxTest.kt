package io.github.rafarolo.outbox.outbox

import io.github.rafarolo.outbox.inbox.Inbox
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.springframework.stereotype.Service
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * H2 rather than a container, so these run on any machine. What is being demonstrated is
 * transactional behaviour, which H2 has; the Kafka half lives in the integration test.
 */
@SpringBootTest(properties = ["spring.task.scheduling.enabled=false"])
class OutboxTest {

    @Autowired lateinit var outbox: Outbox
    @Autowired lateinit var inbox: Inbox
    @Autowired lateinit var orders: OrderService
    @Autowired lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun clean() {
        jdbc.update("DELETE FROM outbox")
        jdbc.update("DELETE FROM inbox")
        jdbc.update("DELETE FROM orders")
    }

    @Test
    fun `a rollback takes the message with it`() {
        runCatching { orders.placeAndFail("order-1") }

        assertEquals(0, outbox.pending())
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
    }

    @Test
    fun `a commit leaves the state and the message together`() {
        orders.place("order-2")

        assertEquals(1, outbox.pending())
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
    }

    @Test
    fun `a message the broker never accepted stays pending`() {
        orders.place("order-3")

        // Nothing marked it published, which is exactly the state a crash between publish
        // and mark leaves behind. The next drain finds it and sends it again.
        assertEquals(1, outbox.pending())
        assertEquals(1, outbox.unpublished().size)
    }

    @Test
    fun `marking is what stops the retry`() {
        orders.place("order-4")
        outbox.markPublished(outbox.unpublished().single().id)

        assertEquals(0, outbox.pending())
    }

    @Test
    fun `the same message is only processed once`() {
        val id = UUID.randomUUID()

        assertTrue(inbox.claim(id, "billing"))
        assertFalse(inbox.claim(id, "billing"))
    }

    @Test
    fun `two consumers claim the same message independently`() {
        val id = UUID.randomUUID()

        // Idempotency is per consumer, not global: billing having handled it says nothing
        // about whether shipping has.
        assertTrue(inbox.claim(id, "billing"))
        assertTrue(inbox.claim(id, "shipping"))
    }
}

@Service
class OrderService(private val outbox: Outbox, private val jdbc: JdbcTemplate) {

    @Transactional
    fun place(id: String) {
        jdbc.update("INSERT INTO orders (id) VALUES (?)", id)
        outbox.record(OutboxMessage(UUID.randomUUID(), id, "orders", """{"id":"$id"}"""))
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun placeAndFail(id: String) {
        jdbc.update("INSERT INTO orders (id) VALUES (?)", id)
        outbox.record(OutboxMessage(UUID.randomUUID(), id, "orders", """{"id":"$id"}"""))
        throw IllegalStateException("the order was rejected downstream")
    }
}
