package io.github.rafarolo.outbox.poison

import org.apache.kafka.common.TopicPartition
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.ExponentialBackOff

@ConfigurationProperties("poison")
data class PoisonProperties(
    val attempts: Int = 4,
    val initialIntervalMillis: Long = 200,
    val multiplier: Double = 2.0,
    val maxIntervalMillis: Long = 5_000,
)

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PoisonProperties::class)
class PoisonConfiguration {

    /**
     * A message that always fails blocks its partition. Not the topic, not the consumer —
     * the partition, and everything queued behind it, which is why the symptom is one
     * customer's events stopping while the rest of the system looks healthy.
     *
     * Retries are therefore bounded and the failure is parked. Unbounded retry is the
     * default reflex and it converts one bad message into an outage.
     */
    @Bean
    fun errorHandler(
        template: KafkaOperations<Any, Any>,
        props: PoisonProperties,
    ): DefaultErrorHandler {
        // Routes to <topic>.DLT, keeping the original partition so the parked record can be
        // traced back to the stream it came from.
        val recoverer = DeadLetterPublishingRecoverer(template) { record, _ ->
            TopicPartition("${record.topic()}.DLT", record.partition())
        }

        val backOff = ExponentialBackOff(props.initialIntervalMillis, props.multiplier).apply {
            maxInterval = props.maxIntervalMillis
            maxAttempts = props.attempts.toLong() - 1
        }

        return DefaultErrorHandler(recoverer, backOff).apply {
            // Deserialization and validation failures cannot succeed on a retry: retrying
            // them just delays the DLT by the whole backoff for no chance of recovery.
            addNotRetryableExceptions(IllegalArgumentException::class.java)
            isAckAfterHandle = true
        }
    }
}
