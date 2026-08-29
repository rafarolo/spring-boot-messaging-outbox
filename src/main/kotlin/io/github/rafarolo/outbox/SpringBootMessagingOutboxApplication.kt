package io.github.rafarolo.outbox

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableScheduling
class SpringBootMessagingOutboxApplication

fun main(args: Array<String>) {
	runApplication<SpringBootMessagingOutboxApplication>(*args)
}
