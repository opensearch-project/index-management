/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement

import org.apache.http.Header
import org.apache.http.HttpEntity
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.client.WarningsHandler
import org.opensearch.commons.authuser.User
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

fun randomDayOfWeekCronField(): String = if (OpenSearchRestTestCase.randomBoolean()) "*" else OpenSearchRestTestCase.randomIntBetween(0, 7).toString()

fun randomMonthCronField(): String = if (OpenSearchRestTestCase.randomBoolean()) "*" else OpenSearchRestTestCase.randomIntBetween(1, 12).toString()

fun randomDayOfMonthCronField(): String = if (OpenSearchRestTestCase.randomBoolean()) "*" else OpenSearchRestTestCase.randomIntBetween(1, 28).toString()

fun randomHourCronField(): String = if (OpenSearchRestTestCase.randomBoolean()) "*" else OpenSearchRestTestCase.randomIntBetween(0, 23).toString()

fun randomMinuteCronField(): String = if (OpenSearchRestTestCase.randomBoolean()) "*" else OpenSearchRestTestCase.randomIntBetween(0, 59).toString()

fun randomCronExpression(): String = "${randomMinuteCronField()} ${randomHourCronField()} ${randomDayOfMonthCronField()} ${randomMonthCronField()} ${randomDayOfWeekCronField()}"

val chronoUnits = listOf(ChronoUnit.MINUTES, ChronoUnit.HOURS, ChronoUnit.DAYS)

fun randomChronoUnit(): ChronoUnit = OpenSearchRestTestCase.randomSubsetOf(1, chronoUnits).first()

fun randomEpochMillis(): Long = OpenSearchRestTestCase.randomLongBetween(0, Instant.now().toEpochMilli())

fun randomInstant(): Instant = Instant.ofEpochMilli(randomEpochMillis())

fun randomCronSchedule(): CronSchedule = CronSchedule(randomCronExpression(), OpenSearchRestTestCase.randomZone())

fun randomIntervalSchedule(): IntervalSchedule = IntervalSchedule(randomInstant(), OpenSearchRestTestCase.randomIntBetween(1, 100), randomChronoUnit())

fun randomSchedule(): Schedule = if (OpenSearchRestTestCase.randomBoolean()) randomIntervalSchedule() else randomCronSchedule()

private fun randomStringList(): List<String> {
    val data = mutableListOf<String>()
    repeat(OpenSearchRestTestCase.randomIntBetween(1, 10)) {
        data.add(OpenSearchRestTestCase.randomAlphaOfLength(10))
    }

    return data
}

fun randomUser(): User {
    return User(OpenSearchRestTestCase.randomAlphaOfLength(10), randomStringList(), randomStringList(), randomStringList())
}

/**
* Wrapper for [RestClient.performRequest] which was deprecated in ES 6.5 and is used in tests. This provides
* a single place to suppress deprecation warnings. This will probably need further work when the API is removed entirely
* but that's an exercise for another day.
*/

fun RestClient.makeRequest(
    method: String,
    endpoint: String,
    params: Map<String, String> = emptyMap(),
    entity: HttpEntity? = null,
    vararg headers: Header,
    strictDeprecationMode: Boolean = false
): Response {
    val request = Request(method, endpoint)
    val options = RequestOptions.DEFAULT.toBuilder()
    options.setWarningsHandler(if (strictDeprecationMode) WarningsHandler.STRICT else WarningsHandler.PERMISSIVE)
    headers.forEach { options.addHeader(it.name, it.value) }
    request.options = options.build()
    params.forEach { request.addParameter(it.key, it.value) }
    if (entity != null) {
        request.entity = entity
    }
    return performRequest(request)
}

/**
 * Wrapper for [RestClient.performRequest] which was deprecated in ES 6.5 and is used in tests. This provides
 * a single place to suppress deprecation warnings. This will probably need further work when the API is removed entirely
 * but that's an exercise for another day.
 */

fun RestClient.makeRequest(
    method: String,
    endpoint: String,
    entity: HttpEntity? = null,
    vararg headers: Header,
    strictDeprecationMode: Boolean = false
): Response {
    val request = Request(method, endpoint)
    val options = RequestOptions.DEFAULT.toBuilder()
    options.setWarningsHandler(if (strictDeprecationMode) WarningsHandler.STRICT else WarningsHandler.PERMISSIVE)
    headers.forEach { options.addHeader(it.name, it.value) }
    request.options = options.build()
    if (entity != null) {
        request.entity = entity
    }
    return performRequest(request)
}

fun <T> waitFor(
    timeout: Instant = Instant.ofEpochSecond(20),
    block: () -> T
): T {
    val startTime = Instant.now().toEpochMilli()
    do {
        try {
            return block()
        } catch (e: Throwable) {
            if ((Instant.now().toEpochMilli() - startTime) > timeout.toEpochMilli()) {
                throw e
            } else {
                Thread.sleep(100L)
            }
        }
    } while (true)
}
