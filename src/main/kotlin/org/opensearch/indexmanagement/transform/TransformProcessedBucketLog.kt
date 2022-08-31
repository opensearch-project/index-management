/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import java.math.BigInteger
import java.security.MessageDigest

class TransformProcessedBucketLog {

    companion object {
        const val MAX_SIZE = 100_000_000
        const val HEX_RADIX = 16
    }

    private var processedBuckets: MutableSet<String> = HashSet()

    fun addBuckets(buckets: List<Map<String, Any>>) {
        buckets.forEach {
            addBucket(it)
        }
    }

    fun addBucket(bucket: Map<String, Any>) {
        if (processedBuckets.size >= MAX_SIZE) return
        processedBuckets.add(computeBucketHash(bucket))
    }

    fun isProcessed(bucket: Map<String, Any>): Boolean {
        return processedBuckets.contains(computeBucketHash(bucket))
    }

    fun isNotProcessed(bucket: Map<String, Any>) = !isProcessed(bucket)

    fun computeBucketHash(bucket: Map<String, Any>): String {
        val md5Crypt = MessageDigest.getInstance("MD5")
        bucket.entries.sortedBy { it.key }.also {
            it.forEach { entry ->
                md5Crypt.update(
                    if (entry.value == null) "null".toByteArray()
                    else entry.value.toString().toByteArray()
                )
            }
        }
        return BigInteger(1, md5Crypt.digest()).toString(HEX_RADIX)
    }
}
