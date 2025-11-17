/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:Suppress("TooManyFunctions", "MatchingDeclarationName")

package org.opensearch.indexmanagement.opensearchapi

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchResponse
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.InjectSecurity
import org.opensearch.commons.authuser.User
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.support.DefaultShardOperationFailedException
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.MediaType
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.DEFAULT_INJECT_ROLES
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.INTERNAL_REQUEST
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.OpenSearchClient
import java.io.IOException
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

fun contentParser(
    bytesReference: BytesReference,
    xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY,
): XContentParser = XContentHelper.createParser(
    xContentRegistry,
    LoggingDeprecationHandler.INSTANCE,
    bytesReference,
    XContentType.JSON,
)

/** Convert an object to maps and lists representation */
fun ToXContent.convertToMap(): Map<String, Any> {
    val bytesReference =
        org.opensearch.core.xcontent.XContentHelper.toXContent(
            this, XContentType.JSON, ToXContent.EMPTY_PARAMS, false,
        )
    return XContentHelper.convertToMap(bytesReference, false, XContentType.JSON as (MediaType)).v2()
}

fun XContentParser.instant(): Instant? = when {
    currentToken() == Token.VALUE_NULL -> null
    currentToken().isValue -> Instant.ofEpochMilli(longValue())
    else -> {
        XContentParserUtils.throwUnknownToken(currentToken(), tokenLocation)
        null // unreachable
    }
}

fun XContentBuilder.aliasesField(aliases: List<Alias>): XContentBuilder {
    val builder = this.startArray(ShrinkAction.ALIASES_FIELD)
    aliases.forEach {
        builder.startObject()
        it.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject()
    }
    return builder.endArray()
}

fun XContentBuilder.optionalTimeField(name: String, instant: Instant?): XContentBuilder {
    if (instant == null) {
        return nullField(name)
    }
    return this.timeField(name, "${name}_in_millis", instant.toEpochMilli())
}

fun XContentBuilder.optionalISMTemplateField(name: String, ismTemplates: List<ISMTemplate>?): XContentBuilder {
    if (ismTemplates == null) {
        return nullField(name)
    }
    return this.field(Policy.ISM_TEMPLATE, ismTemplates.toTypedArray())
}

fun XContentBuilder.optionalUserField(name: String, user: User?): XContentBuilder = if (user == null) nullField(name) else this.field(name, user)

/**
 * Parse data from SearchResponse using the defined parser and xContentRegistry
 */
fun <T> parseFromSearchResponse(
    response: SearchResponse,
    xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY,
    parse: (xcp: XContentParser, id: String, seqNo: Long, primaryTerm: Long) -> T,
): List<T> = response.hits.hits.map {
    val id = it.id
    val seqNo = it.seqNo
    val primaryTerm = it.primaryTerm
    val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, it.sourceRef, XContentType.JSON)
    xcp.parseWithType(id, seqNo, primaryTerm, parse)
}

/**
 * Parse data from GetResponse using the defined parser and xContentRegistry
 */
fun <T> parseFromGetResponse(
    response: GetResponse,
    xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY,
    parse: (xcp: XContentParser, id: String, seqNo: Long, primaryTerm: Long) -> T,
): T {
    val xcp =
        XContentHelper.createParser(
            xContentRegistry,
            LoggingDeprecationHandler.INSTANCE,
            response.sourceAsBytesRef,
            XContentType.JSON,
        )
    return xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, parse)
}

/**
 * Retries the given [block] of code as specified by the receiver [BackoffPolicy],
 * if [block] throws an [OpenSearchException] that is retriable (502, 503, 504).
 *
 * If all retries fail the final exception will be rethrown. Exceptions caught during intermediate retries are
 * logged as warnings to [logger]. Similar to [org.opensearch.action.bulk.Retry], except this retries on
 * 502, 503, 504 error codes as well as 429.
 *
 * @param logger - logger used to log intermediate failures
 * @param retryOn - any additional [RestStatus] values that should be retried
 * @param block - the block of code to retry. This should be a suspend function.
 */
suspend fun <T> BackoffPolicy.retry(
    logger: Logger,
    retryOn: List<RestStatus> = emptyList(),
    block: suspend (backoff: TimeValue) -> T,
): T {
    val iter = iterator()
    var backoff: TimeValue = TimeValue.ZERO
    do {
        try {
            return block(backoff)
        } catch (e: OpenSearchException) {
            if (iter.hasNext() && (e.isRetryable() || retryOn.contains(e.status()))) {
                backoff = iter.next()
                logger.warn("Operation failed. Retrying in $backoff.", e)
                delay(backoff.millis)
            } else {
                throw e
            }
        } catch (rje: OpenSearchRejectedExecutionException) {
            if (iter.hasNext()) {
                backoff = iter.next()
                logger.warn("Rejected execution. Retrying in $backoff.", rje)
                delay((backoff.millis))
            }
        }
    } while (true)
}

/**
 * Retries on 502, 503 and 504 per elastic client's behavior: https://github.com/elastic/elasticsearch-net/issues/2061
 * 429 must be retried manually as it's not clear if it's ok to retry for requests other than Bulk requests.
 */
fun OpenSearchException.isRetryable(): Boolean = (status() in listOf(RestStatus.BAD_GATEWAY, RestStatus.SERVICE_UNAVAILABLE, RestStatus.GATEWAY_TIMEOUT))

/**
 * Extension function for OpenSearch 6.3 and above that duplicates the OpenSearch 6.2 XContentBuilder.string() method.
 */
fun XContentBuilder.string(): String = BytesReference.bytes(this).utf8ToString()

fun XContentBuilder.toMap(): Map<String, Any> = XContentHelper.convertToMap(BytesReference.bytes(this), false, XContentType.JSON as (MediaType)).v2()

/**
 * Converts [OpenSearchClient] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the OpenSearch client API.
 */
suspend fun <C : OpenSearchClient, T> C.suspendUntil(block: C.(ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        block(
            object : ActionListener<T> {
                override fun onResponse(response: T) = cont.resume(response)

                override fun onFailure(e: Exception) = cont.resumeWithException(e)
            },
        )
    }

/**
 * Converts [LockService] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the LockService API.
 */
suspend fun <T> LockService.suspendUntil(block: LockService.(ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        block(
            object : ActionListener<T> {
                override fun onResponse(response: T) = cont.resume(response)

                override fun onFailure(e: Exception) = cont.resumeWithException(e)
            },
        )
    }

/**
 * Converts [NotificationsPluginInterface] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the NotificationsPluginInterface API.
 */
suspend fun <T> NotificationsPluginInterface.suspendUntil(block: NotificationsPluginInterface.(ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        block(
            object : ActionListener<T> {
                override fun onResponse(response: T) = cont.resume(response)

                override fun onFailure(e: Exception) = cont.resumeWithException(e)
            },
        )
    }

fun Throwable.findRemoteTransportException(): RemoteTransportException? {
    if (this is RemoteTransportException) return this
    return this.cause?.findRemoteTransportException()
}

fun DefaultShardOperationFailedException.getUsefulCauseString(): String {
    val rte = this.cause?.findRemoteTransportException()
    return if (rte == null) this.toString() else ExceptionsHelper.unwrapCause(rte).toString()
}

@JvmOverloads
@Throws(IOException::class)
fun <T> XContentParser.parseWithType(
    id: String = NO_ID,
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    parse: (xcp: XContentParser, id: String, seqNo: Long, primaryTerm: Long) -> T,
): T {
    ensureExpectedToken(Token.START_OBJECT, nextToken(), this)
    ensureExpectedToken(Token.FIELD_NAME, nextToken(), this)
    ensureExpectedToken(Token.START_OBJECT, nextToken(), this)
    val parsed = parse(this, id, seqNo, primaryTerm)
    ensureExpectedToken(Token.END_OBJECT, this.nextToken(), this)
    return parsed
}

class IndexManagementSecurityContext(
    private val id: String,
    settings: Settings,
    private val threadContext: ThreadContext,
    private val user: User?,
) : ThreadContextElement<Unit> {
    companion object Key : CoroutineContext.Key<IndexManagementSecurityContext>

    private val logger: Logger = LogManager.getLogger(javaClass)
    override val key: CoroutineContext.Key<*>
        get() = Key
    val injector = InjectSecurity(id, settings, threadContext)

    /**
     * Before the thread executes the coroutine we want the thread context to contain user roles so they are used when executing the code inside
     * the coroutine
     */
    override fun updateThreadContext(context: CoroutineContext) {
        logger.debug("Setting security context in thread ${Thread.currentThread().name} for job $id")
        injector.injectRoles(if (user == null) DEFAULT_INJECT_ROLES else user.roles)
        injector.injectProperty(INTERNAL_REQUEST, true)
    }

    /**
     * Clean up the thread context before the coroutine executed by thread is suspended
     */
    override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
        logger.debug("Cleaning up security context in thread ${Thread.currentThread().name} for job $id")
        injector.close()
    }
}

suspend fun <T> withClosableContext(
    context: IndexManagementSecurityContext,
    block: suspend CoroutineScope.() -> T,
): T {
    try {
        return withContext(context) { block() }
    } finally {
        context.injector.close()
    }
}

fun XContentBuilder.optionalField(name: String, value: Any?): XContentBuilder = if (value != null) {
    this.field(name, value)
} else {
    this
}

fun XContentBuilder.optionalInfoField(name: String, info: SMMetadata.Info?): XContentBuilder = if (info != null) {
    if (info.message != null || info.cause != null) {
        this.field(name, info)
    } else {
        this
    }
} else {
    this
}

inline fun <T> XContentParser.nullValueHandler(block: XContentParser.() -> T): T? = if (currentToken() == Token.VALUE_NULL) null else block()

inline fun <T> XContentParser.parseArray(block: XContentParser.() -> T): List<T> {
    val resArr = mutableListOf<T>()
    ensureExpectedToken(Token.START_ARRAY, currentToken(), this)
    while (nextToken() != Token.END_ARRAY) {
        resArr.add(block())
    }
    return resArr
}

// similar to readOptionalWriteable
fun <T> StreamInput.readOptionalValue(reader: Writeable.Reader<T>): T? = if (readBoolean()) {
    reader.read(this)
} else {
    null
}

fun <T> StreamOutput.writeOptionalValue(value: T, writer: Writeable.Writer<T>) {
    if (value == null) {
        writeBoolean(false)
    } else {
        writeBoolean(true)
        writer.write(this, value)
    }
}
