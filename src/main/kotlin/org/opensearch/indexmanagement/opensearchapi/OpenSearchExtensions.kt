/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

@file:Suppress("TooManyFunctions", "MatchingDeclarationName")

package org.opensearch.indexmanagement.opensearchapi

import kotlinx.coroutines.delay
import org.apache.logging.log4j.Logger
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.support.DefaultShardOperationFailedException
import org.opensearch.client.OpenSearchClient
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.rest.RestStatus
import org.opensearch.transport.RemoteTransportException
import java.io.IOException
import java.time.Instant
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

fun contentParser(bytesReference: BytesReference): XContentParser {
    return XContentHelper.createParser(
        NamedXContentRegistry.EMPTY,
        LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON
    )
}

/** Convert an object to maps and lists representation */
fun ToXContent.convertToMap(): Map<String, Any> {
    val bytesReference = XContentHelper.toXContent(this, XContentType.JSON, false)
    return XContentHelper.convertToMap(bytesReference, false, XContentType.JSON).v2()
}

fun XContentParser.instant(): Instant? {
    return when {
        currentToken() == Token.VALUE_NULL -> null
        currentToken().isValue -> Instant.ofEpochMilli(longValue())
        else -> {
            XContentParserUtils.throwUnknownToken(currentToken(), tokenLocation)
            null // unreachable
        }
    }
}

fun XContentBuilder.aliasesField(aliases: List<Alias>): XContentBuilder {
    val builder = this.startObject(ShrinkActionConfig.ALIASES_FIELD)
    aliases.forEach { it.toXContent(builder, ToXContent.EMPTY_PARAMS) }
    return builder.endObject()
}

fun XContentBuilder.optionalTimeField(name: String, instant: Instant?): XContentBuilder {
    if (instant == null) {
        return nullField(name)
    }
    return this.timeField(name, "${name}_in_millis", instant.toEpochMilli())
}

fun XContentBuilder.optionalISMTemplateField(name: String, ismTemplate: ISMTemplate?): XContentBuilder {
    if (ismTemplate == null) {
        return nullField(name)
    }
    return this.field(Policy.ISM_TEMPLATE, ismTemplate)
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
    block: suspend (backoff: TimeValue) -> T
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
        }
    } while (true)
}

/**
 * Retries on 502, 503 and 504 per elastic client's behavior: https://github.com/elastic/elasticsearch-net/issues/2061
 * 429 must be retried manually as it's not clear if it's ok to retry for requests other than Bulk requests.
 */
fun OpenSearchException.isRetryable(): Boolean {
    return (status() in listOf(RestStatus.BAD_GATEWAY, RestStatus.SERVICE_UNAVAILABLE, RestStatus.GATEWAY_TIMEOUT))
}

/**
 * Extension function for OpenSearch 6.3 and above that duplicates the OpenSearch 6.2 XContentBuilder.string() method.
 */
fun XContentBuilder.string(): String = BytesReference.bytes(this).utf8ToString()

/**
 * Converts [OpenSearchClient] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the OpenSearch client API.
 */
suspend fun <C : OpenSearchClient, T> C.suspendUntil(block: C.(ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        block(object : ActionListener<T> {
            override fun onResponse(response: T) = cont.resume(response)

            override fun onFailure(e: Exception) = cont.resumeWithException(e)
        })
    }

/**
 * Converts [LockService] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the LockService API.
 */
suspend fun <T> LockService.suspendUntil(block: LockService.(ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        block(object : ActionListener<T> {
            override fun onResponse(response: T) = cont.resume(response)

            override fun onFailure(e: Exception) = cont.resumeWithException(e)
        })
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
    parse: (xcp: XContentParser, id: String, seqNo: Long, primaryTerm: Long) -> T
): T {
    ensureExpectedToken(Token.START_OBJECT, nextToken(), this)
    ensureExpectedToken(Token.FIELD_NAME, nextToken(), this)
    ensureExpectedToken(Token.START_OBJECT, nextToken(), this)
    val parsed = parse(this, id, seqNo, primaryTerm)
    ensureExpectedToken(Token.END_OBJECT, this.nextToken(), this)
    return parsed
}
