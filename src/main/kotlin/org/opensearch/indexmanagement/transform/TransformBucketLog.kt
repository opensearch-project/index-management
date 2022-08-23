/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.logging.log4j.LogManager
import java.math.BigInteger
import java.security.MessageDigest

class TransformBucketLog {

    private val maxSize: Int = 100_000_000
    private val mutex = Mutex()

    private var processedBuckets: MutableSet<String> = HashSet()

    private var logger = LogManager.getLogger(javaClass)

    suspend fun addBuckets(buckets: List<Map<String, Any>>) {
        buckets.forEach {
            addBucket(it)
        }
    }

    suspend fun addBucket(bucket: Map<String, Any>) {
        mutex.withLock {
            if (processedBuckets.size >= maxSize) return
            processedBuckets.add(computeBucketHash(bucket))
        }
    }

    suspend fun isProcessed(bucket: Map<String, Any>): Boolean {
        mutex.withLock {
            return processedBuckets.contains(computeBucketHash(bucket))
        }
    }

    suspend fun isNotProcessed(bucket: Map<String, Any>) = !isProcessed(bucket)

    suspend fun computeBucketHash(bucket: Map<String, Any>): String {
        val md5Crypt = MessageDigest.getInstance("MD5")
        bucket.entries.sortedBy { it.key }.also {
            it.forEach { entry ->
                md5Crypt.update(entry.value.toString().toByteArray())
            }
        }
        return BigInteger(1, md5Crypt.digest()).toString(16)
    }
}
