/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import java.math.BigInteger
import java.security.MessageDigest

class TransformProcessedBucketLog {

    private val maxSize: Int = 100_000_000

    private var processedBuckets: MutableSet<String> = HashSet()

    fun addBuckets(buckets: List<Map<String, Any>>) {
        buckets.forEach {
            addBucket(it)
        }
    }

    fun addBucket(bucket: Map<String, Any>) {
        if (processedBuckets.size >= maxSize) return
        processedBuckets.add(computeBucketHash(bucket))
    }

    fun isProcessed(bucket: Map<String, Any>): Boolean {
        return processedBuckets.contains(computeBucketHash(bucket))
    }

    suspend fun isNotProcessed(bucket: Map<String, Any>) = !isProcessed(bucket)

    fun computeBucketHash(bucket: Map<String, Any>): String {
        val md5Crypt = MessageDigest.getInstance("MD5")
        bucket.entries.sortedBy { it.key }.also {
            it.forEach { entry ->
                md5Crypt.update(entry.value.toString().toByteArray())
            }
        }
        return BigInteger(1, md5Crypt.digest()).toString(16)
    }
}
