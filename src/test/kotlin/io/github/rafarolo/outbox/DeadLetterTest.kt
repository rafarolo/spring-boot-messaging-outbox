package io.github.rafarolo.outbox

import io.github.rafarolo.outbox.consumer.OrderConsumer
import io.github.rafarolo.outbox.outbox.OrderService
import io.github.rafarolo.outbox.outbox.Outbox
import io.github.rafarolo.outbox.outbox.OutboxMessage
import io.github.rafarolo.outbox.outbox.OutboxPublisher
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIf
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.DockerClientFactory
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Needs a broker, so it skips where there is no Docker and runs in CI. Everything that can
 * be proven without one already is, in OutboxTest and PoisonPolicyTest.
 */
@Import(TestcontainersConfiguration::class)
@SpringBootTest(properties = ["spring.task.scheduling.enabled=false"])
@EnabledIf("dockerAvailable")
class DeadLetterTest {

    @Autowired lateinit var orders: OrderService
    @Autowired lateinit var outbox: Outbox
    @Autowired lateinit var publisher: OutboxPublisher
    @Autowired lateinit var consumer: OrderConsumer
    @Autowired lateinit var kafka: KafkaTemplate<String, String>
    @Autowired lateinit var jdbc: JdbcTemplate

    private fun reset() {
        jdbc.update("DELETE FROM outbox"); jdbc.update("DELETE FROM inbox"); jdbc.update("DELETE FROM orders")
        consumer.applied.clear(); consumer.attempts.clear()
    }

    @Test
    fun `an order written in a transaction reaches the consumer`() {
        reset()
        orders.place("order-a")
        publisher.drain()

        await().atMost(Duration.ofSeconds(20)).until { consumer.applied.contains("order-a") }
        assertEquals(0, outbox.pending())
    }

    @Test
    fun `redelivery of the same record applies once`() {
        reset()
        val id = UUID.randomUUID()
        val payload = """{"id":"order-b"}"""
        outbox.record(OutboxMessage(id, "order-b", "orders", payload))
        publisher.drain()
        await().atMost(Duration.ofSeconds(20)).until { consumer.applied.contains("order-b") }

        // The same record again, exactly as a broker redelivers after a slow acknowledgement.
        kafka.send("orders", "order-b", payload).get()

        await().atMost(Duration.ofSeconds(10)).until { (consumer.attempts["order-b"] ?: 0) >= 2 }
        assertEquals(1, consumer.applied.count { it == "order-b" })
    }

    @Test
    fun `a poison record is parked and the partition keeps moving`() {
        reset()
        kafka.send("orders", "order-poison", """{"id":"order-poison","poison":true}""").get()
        kafka.send("orders", "order-c", """{"id":"order-c"}""").get()

        // The healthy record behind the poison one still arrives, which is the whole point:
        // a bounded retry parks the failure instead of blocking everything queued behind it.
        await().atMost(Duration.ofSeconds(30)).until { consumer.applied.contains("order-c") }
        assertTrue(consumer.applied.none { it == "order-poison" })
    }

    companion object {
        @JvmStatic
        fun dockerAvailable(): Boolean = DockerClientFactory.instance().isDockerAvailable
    }
}
