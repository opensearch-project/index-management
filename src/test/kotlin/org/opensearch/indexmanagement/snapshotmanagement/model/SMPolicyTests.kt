/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.indexmanagement.snapshotmanagement.randomSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.toJsonString
import org.opensearch.test.OpenSearchTestCase

class SMPolicyTests : OpenSearchTestCase() {

    fun `test policy with optional creation field`() {
        val deletionOnlyPolicy = randomSMPolicy(
            creationNull = true,
            deletionMaxAge = TimeValue.timeValueDays(7),
            deletionMinCount = 3,
        )

        assertNull("Creation should be null for deletion-only policy", deletionOnlyPolicy.creation)
        assertNotNull("Deletion should not be null", deletionOnlyPolicy.deletion)
    }

    fun `test policy with optional snapshotPattern field`() {
        val policyWithPattern = randomSMPolicy(
            deletionMaxAge = TimeValue.timeValueDays(7),
            deletionMinCount = 3,
            snapshotPattern = "backup-*",
        )

        assertEquals("Snapshot pattern should match", "backup-*", policyWithPattern.deletion?.snapshotPattern)
    }

    fun `test policy serialization with optional creation`() {
        val deletionOnlyPolicy = randomSMPolicy(
            creationNull = true,
            deletionMaxAge = TimeValue.timeValueDays(7),
            deletionMinCount = 3,
            snapshotPattern = "pattern-*",
        )

        val out = BytesStreamOutput()
        deletionOnlyPolicy.writeTo(out)

        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val deserializedPolicy = SMPolicy(sin)

        assertNull("Deserialized creation should be null", deserializedPolicy.creation)
        assertEquals("Snapshot pattern should be preserved", "pattern-*", deserializedPolicy.deletion?.snapshotPattern)
    }

    fun `test policy parsing from JSON with optional fields`() {
        val jsonWithOptionalFields = """
        {
            "name": "test-policy",
            "description": "Test policy with optional creation",
            "deletion": {
                "schedule": {
                    "cron": {
                        "expression": "0 0 * * *",
                        "timezone": "UTC"
                    }
                },
                "condition": {
                    "max_age": "7d",
                    "min_count": 3
                },
                "snapshot_pattern": "external-*"
            },
            "snapshot_config": {
                "repository": "test-repo"
            },
            "enabled": true
        }
        """.trimIndent()

        val policy = SMPolicy.parse(createParser(XContentType.JSON.xContent(), jsonWithOptionalFields), "test-policy-id")

        assertNull("Creation should be null when not specified", policy.creation)
        assertNotNull("Deletion should not be null", policy.deletion)
        assertEquals("Snapshot pattern should match", "external-*", policy.deletion?.snapshotPattern)
    }

    fun `test policy toXContent with optional fields`() {
        val policy = randomSMPolicy(
            creationNull = true,
            deletionMaxAge = TimeValue.timeValueDays(7),
            deletionMinCount = 3,
            snapshotPattern = "backup-*",
        )

        val jsonString = policy.toJsonString()

        assertFalse("JSON should not contain creation field", jsonString.contains("\"creation\""))
        assertTrue("JSON should contain deletion field", jsonString.contains("\"deletion\""))
        assertTrue("JSON should contain snapshot_pattern", jsonString.contains("\"snapshot_pattern\""))
        assertTrue("JSON should contain backup-* pattern", jsonString.contains("\"backup-*\""))
    }

    fun `test policy validation requires either creation or deletion`() {
        assertThrows("Policy with neither creation nor deletion should fail validation", IllegalArgumentException::class.java) {
            SMPolicy(
                id = "test-id",
                description = "Invalid policy",
                schemaVersion = 1L,
                creation = null,
                deletion = null,
                snapshotConfig = mapOf("repository" to "test-repo"),
                jobEnabled = true,
                jobLastUpdateTime = randomInstant(),
                jobEnabledTime = randomInstant(),
                jobSchedule = randomCronSchedule(),
            )
        }
    }

    fun `test deletion-only policy round-trip serialization`() {
        val originalPolicy = randomSMPolicy(
            creationNull = true,
            deletionMaxAge = TimeValue.timeValueDays(30),
            deletionMinCount = 5,
            deletionMaxCount = 100,
            snapshotPattern = "daily-*",
        )

        // Test JSON round-trip
        val jsonString = originalPolicy.toJsonString()
        val parsedPolicy = SMPolicy.parse(createParser(XContentType.JSON.xContent(), jsonString), originalPolicy.id)

        assertNull("Parsed creation should be null", parsedPolicy.creation)
        assertEquals("Deletion condition should match", originalPolicy.deletion?.condition, parsedPolicy.deletion?.condition)
        assertEquals("Snapshot pattern should match", originalPolicy.deletion?.snapshotPattern, parsedPolicy.deletion?.snapshotPattern)
    }

    private fun randomCronSchedule() = org.opensearch.jobscheduler.spi.schedule.CronSchedule("0 0 * * *", java.time.ZoneId.of("UTC"))

    private fun randomInstant() = java.time.Instant.now()
}
