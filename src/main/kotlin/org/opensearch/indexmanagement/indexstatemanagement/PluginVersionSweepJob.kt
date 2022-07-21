/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

class PluginVersionSweepJob constructor(
    private val intervalInMinutes: Int,
) : CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.IO + CoroutineName("SweepISMCoordinator")) {
    // Lateinit - in order to do injection of the sweep checker component in visible manner
    lateinit var sweep: suspend () -> Boolean
    private val eventChannel = Channel<Boolean>(CHANNEL_CAPACITY)
    private var waitAndScheduleSweepJob: Job? = null

    init {
        launch { listenForClusterChanged() }
    }

    fun scheduleSweep() {
        launch {
            eventChannel.send(true)
        }
    }

    fun cancelSweepJob() {
        this.cancel()
    }

    private suspend fun listenForClusterChanged() {
        while (isActive) {
            eventChannel.receive()
            val skipFlag = sweep()
            // If the flag is set to true - new coroutine that tries to send event after given interval
            // will be scheduled on event loop
            if (skipFlag) {
                if (!isActiveWaitAndScheduleSweepJob()) {
                    waitAndScheduleSweepJob = launch { waitAndScheduleSweep() }
                }
            } else {
                if (isActiveWaitAndScheduleSweepJob()) {
                    waitAndScheduleSweepJob?.cancel()
                }
            }
        }
    }

    private suspend fun waitAndScheduleSweep() {
        delay(ONE_MINUTE_IN_MILLIS * intervalInMinutes)
        eventChannel.send(true)
    }

    private fun isActiveWaitAndScheduleSweepJob() = waitAndScheduleSweepJob != null && waitAndScheduleSweepJob!!.isActive

    companion object {
        const val ONE_MINUTE_IN_MILLIS = 60000L
        private const val CHANNEL_CAPACITY = 50
    }
}
