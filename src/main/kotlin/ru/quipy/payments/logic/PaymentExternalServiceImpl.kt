package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.CoroutineRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.lang.Double.min
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    properties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val cost = properties.cost

    private val _rateLimiter = CoroutineRateLimiter(rateLimitPerSec, TimeUnit.SECONDS)
    override val rateLimiter: CoroutineRateLimiter
        get() = _rateLimiter

    private val _window = NonBlockingOngoingWindow(parallelRequests)
    override val window: NonBlockingOngoingWindow
        get() = _window

    private val requestCounter = NonBlockingOngoingWindow(parallelRequests)
    private val failureCounter = NonBlockingOngoingWindow(parallelRequests)
    private val responseCounter = NonBlockingOngoingWindow(parallelRequests)

    override val getCost: Double
        get() = cost

    private fun maxQueries() =
        ((paymentOperationTimeout.toMillis() - requestAverageProcessingTime.toMillis())
                * getSpeed() * parallelRequests).toInt()

    private val _queries = ArrayBlockingQueue<PaymentInfo>(maxQueries())

    override val getQueries: ArrayBlockingQueue<PaymentInfo>
        get() = _queries

    private val _failures = ArrayBlockingQueue<FailureInfo>(maxQueries())

    private val _responses = ArrayBlockingQueue<ResponseInfo>(maxQueries())

    private val queueProcessingExecutor = Executors.newFixedThreadPool(100)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newCachedThreadPool()

    private val client = OkHttpClient.Builder().run {
        val dis = Dispatcher(httpClientExecutor)
        dis.maxRequestsPerHost = parallelRequests
        dis.maxRequests = parallelRequests
        dispatcher(dis)
        protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        build()
    }

    override fun getSpeed(): Double {
        val averageTime = requestAverageProcessingTime.toMillis().toDouble()
        return min(
            parallelRequests.toDouble() / averageTime,
            rateLimitPerSec.toDouble() / 1000
        )
    }

    override fun canWait(paymentStartedAt: Long): Boolean {
        return paymentOperationTimeout - Duration.ofMillis(now() - paymentStartedAt) >=
                requestAverageProcessingTime.multipliedBy(2)
    }

    override fun notOverTime(paymentStartedAt: Long): Boolean {
        return Duration.ofMillis(now() - paymentStartedAt) + requestAverageProcessingTime < paymentOperationTimeout
    }

    private fun submitPaymentRequest(
        transactionId: UUID, paymentId: UUID, paymentStartedAt: Long
    ) = queueProcessingExecutor.execute {

        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = enqueueFailure(
                call, e, paymentId, transactionId
            )

            override fun onResponse(call: Call, response: Response) = enqueueResponse(
                call, response, paymentId, transactionId, paymentStartedAt
            )
        })
    }

    override fun enqueueQuery(
        paymentId: UUID, amount: Int, paymentStartedAt: Long
    ) {
        queueProcessingExecutor.execute {
            _queries.put(PaymentInfo(paymentId, amount, paymentStartedAt))
        }
    }

    fun enqueueFailure(
        call: Call, e: IOException, paymentId: UUID, transactionId: UUID
    ) {
        queueProcessingExecutor.execute {
            _failures.put(FailureInfo(call, e, paymentId, transactionId))
        }
    }

    fun enqueueResponse(
        call: Call, response: Response, paymentId: UUID, transactionId: UUID, paymentStartedAt: Long,
    ) {
        queueProcessingExecutor.execute {
            _responses.put(ResponseInfo(call, response, paymentId, transactionId, paymentStartedAt))
        }
    }

    private val processQueriesQueue = queueProcessingExecutor.execute {
        while (true) {
            if (_queries.isNotEmpty()) {
                val windowResult = requestCounter.putIntoWindow()
                if (windowResult is NonBlockingOngoingWindow.WindowResponse.Success) {
                    while (!rateLimiter.tick()) {
                        continue
                    }
                } else {
                    continue
                }
            } else {
                continue
            }

            val payment = _queries.take()
            logger.warn("[${accountName}] Submitting payment request for payment ${payment.id}. Already passed: ${now() - payment.startedAt} ms")
            val transactionId = UUID.randomUUID()
            logger.info("[${accountName}] Submit for ${payment.id} , txId: $transactionId")
            paymentESService.update(payment.id) {
                it.logSubmission(
                    success = true, transactionId, now(), Duration.ofMillis(now() - payment.startedAt)
                )
            }

            submitPaymentRequest(transactionId, payment.id, payment.startedAt)
        }
    }

    private val processFailureQueue = queueProcessingExecutor.execute {
        while (true) {
            if (_failures.isNotEmpty()) {
                val windowResult = failureCounter.putIntoWindow()
                if (windowResult is NonBlockingOngoingWindow.WindowResponse.Success) {
                    while (!rateLimiter.tick()) {
                        continue
                    }
                } else {
                    continue
                }
            } else {
                continue
            }

            val failure = _failures.take()
            val paymentId = failure.paymentId
            val transactionId = failure.transactionId
            when (failure.e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        failure.e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = failure.e.message)
                    }
                }
            }
            window.releaseWindow()
        }
    }

    private val processResponseQueue = queueProcessingExecutor.execute {
        while (true) {
            if (_responses.isNotEmpty()) {
                val windowResult = responseCounter.putIntoWindow()
                if (windowResult is NonBlockingOngoingWindow.WindowResponse.Success) {
                    while (!rateLimiter.tick()) {
                        continue
                    }
                } else {
                    continue
                }
            } else {
                continue
            }

            val response = _responses.take()

            val paymentId = response.paymentId
            val transactionId = response.transactionId
            val paymentStartedAt = response.paymentStartedAt

            response.response.use {
                val body = try {
                    mapper.readValue(response.response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.response.code}, reason: ${response.response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, duration: ${now() - paymentStartedAt}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
            window.releaseWindow()
        }
    }

    data class PaymentInfo(
        val id: UUID, val amount: Int, val startedAt: Long
    )

    data class FailureInfo(
        val call: Call, val e: IOException, val paymentId: UUID, val transactionId: UUID
    )

    data class ResponseInfo(
        val call: Call, val response: Response, val paymentId: UUID, val transactionId: UUID, val paymentStartedAt: Long
    )
}

fun now() = System.currentTimeMillis()