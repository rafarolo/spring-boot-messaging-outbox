package io.github.rafarolo.outbox.outbox

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class OutboxPublisher(
    private val outbox: Outbox,
    private val kafka: KafkaTemplate<String, String>,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Publishes, then marks. The other order loses messages: mark first and a failure to
     * publish leaves a row that will never be retried, which is the one outcome the outbox
     * exists to prevent.
     *
     * The aggregate is the partition key, so everything about one aggregate keeps its order.
     * Without it the broker spreads them and a correction can overtake the thing it corrects.
     */
    @Scheduled(fixedDelayString = "\${outbox.poll-interval:PT1S}")
    fun drain() {
        for (message in outbox.unpublished()) {
            try {
                kafka.send(message.topic, message.aggregate, message.payload).get()
                outbox.markPublished(message.id)
            } catch (e: Exception) {
                // Left unmarked on purpose: the next pass picks it up again. This is where
                // at-least-once comes from, and why the consumer has to be idempotent.
                log.warn("outbox message {} stays pending: {}", message.id, e.message)
                return
            }
        }
    }
}
