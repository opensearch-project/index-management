/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.math.BigInteger
import java.security.MessageDigest

class TransformBucketsStore {

    companion object {
        const val MAX_SIZE = 100_000_000
        const val HEX_RADIX = 16
    }

    private var bucketHashSet: MutableSet<String> = HashSet()
    private var bucketList: MutableList<Map<String, Any>> = ArrayList()

    private val mutex = Mutex()

    suspend fun addBuckets(buckets: Collection<Map<String, Any>>) {
        mutex.withLock {
            buckets.forEach {
                if (bucketHashSet.size >= MAX_SIZE) return
                bucketHashSet += computeBucketHash(it)
                bucketList += it
            }
        }
    }

    suspend fun addBucket(bucket: Map<String, Any>) {
        mutex.withLock {
            if (bucketHashSet.size >= MAX_SIZE) return
            bucketHashSet.add(computeBucketHash(bucket))
        }
    }

    suspend fun getAndRemove(n: Int): List<String> {
        val buckets = mutableListOf<String>()
        mutex.withLock {
            val iter = bucketHashSet.iterator()
            while (iter.hasNext() || buckets.size < n) {
                buckets.add(iter.next())
                iter.remove()
            }
        }
        return buckets
    }

    private fun isProcessed(bucket: Map<String, Any>): Boolean {
        return bucketHashSet.contains(computeBucketHash(bucket))
    }

    fun isNotProcessed(bucket: Map<String, Any>) = !isProcessed(bucket)

    private fun computeBucketHash(bucket: Map<String, Any?>): String {
        val md5Crypt = MessageDigest.getInstance("MD5")
        bucket.entries.sortedBy { it.key }.onEach { entry ->
            md5Crypt.update(
                if (entry.value == null) "null".toByteArray()
                else entry.value.toString().toByteArray()
            )
        }
        return BigInteger(1, md5Crypt.digest()).toString(HEX_RADIX)
    }

    fun getAllBuckets(): List<Map<String, Any>> {
        return bucketList
    }
}
