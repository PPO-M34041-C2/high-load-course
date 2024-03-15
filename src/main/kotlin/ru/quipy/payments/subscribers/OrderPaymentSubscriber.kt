package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.logic.*
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.PostConstruct

@Service
class OrderPaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    @Qualifier(ExternalServicesConfig.FIRST_PAYMENT_BEAN)
    private lateinit var firstPaymentService: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.SECOND_PAYMENT_BEAN)
    private lateinit var secondPaymentService: PaymentService

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(OrderAggregate::class, "payments:order-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
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

                    while(true) {

                        if (secondPaymentService.notOverTime(event.createdAt)){
                            if (secondPaymentService.window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                                if (secondPaymentService.rateLimiter.tick()) {
                                    secondPaymentService.submitPaymentRequest(createdEvent.paymentId, event.amount, event.createdAt)
                                    break
                                } else {
                                    secondPaymentService.window.releaseWindow()
                                }
                            }
                        }

                        //Проверка на возможность ожидания, пока аккаунт не освободится (изи ~ +5% кэша)
                        if (secondPaymentService.canWait(event.createdAt))
                            continue

                        if (firstPaymentService.notOverTime(event.createdAt)){
                            if (firstPaymentService.window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                                if (firstPaymentService.rateLimiter.tick()) {
                                    firstPaymentService.submitPaymentRequest(createdEvent.paymentId, event.amount, event.createdAt)
                                } else {
                                    firstPaymentService.window.releaseWindow()
                                }
                            }
                        }

                        if (firstPaymentService.canWait(event.createdAt))
                            continue
                        else {
                            paymentESService.update(createdEvent.paymentId) {
                                val transactionId = UUID.randomUUID()
                                logger.warn("${createdEvent.paymentId} не смог оплатиться")
                                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - event.createdAt))
                                it.logProcessing(success = false, processedAt = now(), transactionId = transactionId)
                            }
                            break
                        }
                    }
                }
            }
        }
    }
}