package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import javax.annotation.PostConstruct

@Service
class OrderPaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentServices: List<PaymentService>

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))

    private var nearestTimes = longArrayOf(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)

    @PostConstruct
    fun init() {
        paymentServices = paymentServices.sortedBy { it.getCost }

        subscriptionsManager.createSubscriber(
            OrderAggregate::class,
            "payments:order-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(
                            event.paymentId,
                            event.orderId,
                            event.amount
                        )
                    }
                    logger.info("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")

                    outerCycle@ while (true) {
                        for (index in paymentServices.indices) {
                            if (paymentServices[index].getQueries.remainingCapacity() == 0) continue
                            if (paymentServices[index].window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                                if (paymentServices[index].rateLimiter.tick()) {
                                    nearestTimes[index] =
                                        (System.currentTimeMillis() + (1.0 / paymentServices[index].getSpeed())).toLong()
                                    paymentServices[index].enqueueQuery(
                                        createdEvent.paymentId,
                                        event.amount,
                                        event.createdAt
                                    )
                                    break@outerCycle
                                } else {
                                    paymentServices[index].window.releaseWindow()
                                }
                            }
                        }
                        paymentESService.update(createdEvent.paymentId) {
                            val transactionId = UUID.randomUUID()
                            logger.warn("${createdEvent.paymentId} не смог оплатиться")
                            it.logSubmission(
                                success = true,
                                transactionId,
                                now(),
                                Duration.ofMillis(now() - event.createdAt)
                            )
                            it.logProcessing(success = false, processedAt = now(), transactionId = transactionId)
                        }
                        break
                    }
                }
            }
        }
    }
}