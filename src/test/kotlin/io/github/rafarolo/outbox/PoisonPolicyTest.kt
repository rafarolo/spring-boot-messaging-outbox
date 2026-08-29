package io.github.rafarolo.outbox

import io.github.rafarolo.outbox.poison.PoisonProperties
import org.junit.jupiter.api.Test
import org.springframework.util.backoff.BackOffExecution
import org.springframework.util.backoff.ExponentialBackOff
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Runs without a broker, so the retry budget is checked on every machine. What it protects
 * is the property that matters: the number of attempts is finite.
 */
class PoisonPolicyTest {

    @Test
    fun `the retry budget is bounded`() {
        val props = PoisonProperties()
        val backOff = ExponentialBackOff(props.initialIntervalMillis, props.multiplier).apply {
            maxInterval = props.maxIntervalMillis
            maxAttempts = props.attempts.toLong() - 1
        }

        val execution = backOff.start()
        val waits = generateSequence { execution.nextBackOff() }
            .takeWhile { it != BackOffExecution.STOP }
            .toList()

        // Three retries after the first delivery: four attempts in total, then the record
        // is parked. Unbounded retry is the default reflex and it turns one bad message
        // into an outage on the partition it is stuck in.
        assertEquals(props.attempts - 1, waits.size)
        assertTrue(waits.all { it <= props.maxIntervalMillis })
        assertTrue(waits.zipWithNext().all { (a, b) -> b >= a })
    }
}
