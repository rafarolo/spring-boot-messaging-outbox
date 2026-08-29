package io.github.rafarolo.outbox.consumer

import io.github.rafarolo.outbox.inbox.Inbox
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Component
class OrderConsumer(private val inbox: Inbox) {

    private val log = LoggerFactory.getLogger(javaClass)

    /** What the consumer actually did, so a test can count effects rather than calls. */
    val applied: MutableSet<String> = ConcurrentHashMap.newKeySet()
    val attempts = ConcurrentHashMap<String, Int>()

    @KafkaListener(topics = ["orders"], groupId = "billing")
    fun onOrder(record: ConsumerRecord<String, String>, @Header(KafkaHeaders.RECEIVED_KEY) key: String) {
        val messageId = UUID.nameUUIDFromBytes("${record.topic()}:${record.offset()}".toByteArray())
        attempts.merge(key, 1, Int::plus)

        // Claimed before the work, not after. Claiming after leaves a window where a crash
        // repeats an effect that already happened, which is the failure the inbox exists
        // to close.
        if (!inbox.claim(messageId, "billing")) {
            log.debug("order {} already applied, skipping", key)
            return
        }

        require(!record.value().contains("\"poison\":true")) { "order $key cannot be applied" }
        applied.add(key)
    }
}
