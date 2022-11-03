/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.update.UpdateResponse
import org.opensearch.client.Client
import org.opensearch.cluster.health.ClusterHealthStatus
import org.opensearch.cluster.health.ClusterStateHealth
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.action.TransitionsAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getManagedIndexMetadata
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ALLOW_LIST_NONE
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.DEFAULT_ISM_ENABLED
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.DEFAULT_JOB_INTERVAL
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.DEFAULT_ACTION_VALIDATION_ENABLED
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.INDEX_STATE_MANAGEMENT_ENABLED
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.JOB_INTERVAL
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings.Companion.ACTION_VALIDATION_ENABLED
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.MetadataCheck
import org.opensearch.indexmanagement.indexstatemanagement.util.checkMetadata
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexMetadataRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.deleteManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.getCompletedManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.util.getStartingManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.util.hasDifferentJobInterval
import org.opensearch.indexmanagement.indexstatemanagement.util.hasTimedOut
import org.opensearch.indexmanagement.indexstatemanagement.util.hasVersionConflict
import org.opensearch.indexmanagement.indexstatemanagement.util.isAllowed
import org.opensearch.indexmanagement.indexstatemanagement.util.isFailed
import org.opensearch.indexmanagement.indexstatemanagement.util.isSafeToChange
import org.opensearch.indexmanagement.indexstatemanagement.util.isSuccessfulDelete
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexConfigIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.publishLegacyNotification
import org.opensearch.indexmanagement.indexstatemanagement.util.sendNotification
import org.opensearch.indexmanagement.indexstatemanagement.util.shouldBackoff
import org.opensearch.indexmanagement.indexstatemanagement.util.shouldChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.util.updateDisableManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.validation.ActionValidation
import org.opensearch.indexmanagement.opensearchapi.IndexManagementSecurityContext
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.retry
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.opensearchapi.withClosableContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.Validate
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.LockModel
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.TemplateScript
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import java.time.temporal.ChronoUnit

@Suppress("TooManyFunctions", "LargeClass")
object ManagedIndexRunner :
    ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("ManagedIndexRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var clusterService: ClusterService
    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var scriptService: ScriptService
    private lateinit var settings: Settings
    private lateinit var imIndices: IndexManagementIndices
    lateinit var actionValidation: ActionValidation
    private lateinit var ismHistory: IndexStateManagementHistory
    private lateinit var skipExecFlag: SkipExecution
    private lateinit var threadPool: ThreadPool
    private lateinit var extensionStatusChecker: ExtensionStatusChecker
    private lateinit var indexMetadataProvider: IndexMetadataProvider
    private var indexStateManagementEnabled: Boolean = DEFAULT_ISM_ENABLED
    private var validationServiceEnabled: Boolean = DEFAULT_ACTION_VALIDATION_ENABLED
    @Suppress("MagicNumber")
    private val savePolicyRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(250), 3)
    @Suppress("MagicNumber")
    private val updateMetaDataRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(250), 3)
    @Suppress("MagicNumber")
    private val errorNotificationRetryPolicy = BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(250), 3)
    private var jobInterval: Int = DEFAULT_JOB_INTERVAL
    private var allowList: List<String> = ALLOW_LIST_NONE

    fun registerClusterService(clusterService: ClusterService): ManagedIndexRunner {
        this.clusterService = clusterService
        return this
    }

    fun registerClient(client: Client): ManagedIndexRunner {
        this.client = client
        return this
    }

    fun registerNamedXContentRegistry(xContentRegistry: NamedXContentRegistry): ManagedIndexRunner {
        this.xContentRegistry = xContentRegistry
        return this
    }

    fun registerScriptService(scriptService: ScriptService): ManagedIndexRunner {
        this.scriptService = scriptService
        return this
    }

    fun registerSettings(settings: Settings): ManagedIndexRunner {
        this.settings = settings
        return this
    }

    // must be called after registerSettings and registerClusterService in IndexManagementPlugin
    fun registerConsumers(): ManagedIndexRunner {
        jobInterval = JOB_INTERVAL.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(JOB_INTERVAL) {
            jobInterval = it
        }

        indexStateManagementEnabled = INDEX_STATE_MANAGEMENT_ENABLED.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_STATE_MANAGEMENT_ENABLED) {
            indexStateManagementEnabled = it
        }

        validationServiceEnabled = ACTION_VALIDATION_ENABLED.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ACTION_VALIDATION_ENABLED) {
            validationServiceEnabled = it
        }

        allowList = ALLOW_LIST.get(settings)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) {
            allowList = it
        }

        return this
    }

    fun registerIMIndex(imIndices: IndexManagementIndices): ManagedIndexRunner {
        this.imIndices = imIndices
        return this
    }

    fun registerValidationService(actionValidation: ActionValidation): ManagedIndexRunner {
        this.actionValidation = actionValidation
        return this
    }

    fun registerHistoryIndex(ismHistory: IndexStateManagementHistory): ManagedIndexRunner {
        this.ismHistory = ismHistory
        return this
    }

    fun registerSkipFlag(flag: SkipExecution): ManagedIndexRunner {
        this.skipExecFlag = flag
        return this
    }

    fun registerThreadPool(threadPool: ThreadPool): ManagedIndexRunner {
        this.threadPool = threadPool
        return this
    }

    fun registerIndexMetadataProvider(indexMetadataProvider: IndexMetadataProvider): ManagedIndexRunner {
        this.indexMetadataProvider = indexMetadataProvider
        return this
    }

    fun registerExtensionChecker(extensionStatusChecker: ExtensionStatusChecker): ManagedIndexRunner {
        this.extensionStatusChecker = extensionStatusChecker
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is ManagedIndexConfig) {
            throw IllegalArgumentException("Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}")
        }

        launch {
            if (skipExecFlag.flag) {
                logger.info("Cluster still has nodes running old version ISM plugin, skip execution on new nodes until all nodes upgraded")
                return@launch
            }

            // Attempt to acquire lock
            val lock: LockModel? = context.lockService.suspendUntil { acquireLock(job, context, it) }
            if (lock == null) {
                logger.debug("Could not acquire lock [${lock?.lockId}] for ${job.index}")
            } else {
                runManagedIndexConfig(job, context)
                // Release lock
                val released: Boolean = context.lockService.suspendUntil { release(lock, it) }
                if (!released) {
                    logger.debug("Could not release lock [${lock.lockId}] for ${job.index}")
                }
            }
        }
    }

    @Suppress("ReturnCount", "ComplexMethod", "LongMethod", "ComplexCondition", "NestedBlockDepth")
    private suspend fun runManagedIndexConfig(managedIndexConfig: ManagedIndexConfig, jobContext: JobExecutionContext) {
        logger.debug("Run job for index ${managedIndexConfig.index}")
        // doing a check of local cluster health as we do not want to overload cluster manager node with potentially a lot of calls
        if (clusterIsRed()) {
            logger.debug("Skipping current execution of ${managedIndexConfig.index} because of red cluster health")
            return
        }

        val (managedIndexMetaData, getMetadataSuccess) = client.getManagedIndexMetadata(managedIndexConfig.indexUuid)
        if (!getMetadataSuccess) {
            logger.info("Failed to retrieve managed index metadata of index [${managedIndexConfig.index}] from config index, abort this run.")
            return
        }

        // Check the cluster state for the index metadata
        var clusterStateIndexMetadata = getIndexMetadata(managedIndexConfig.index)
        val defaultIndexMetadataService = indexMetadataProvider.services[DEFAULT_INDEX_TYPE] as DefaultIndexMetadataService
        val clusterStateIndexUUID = clusterStateIndexMetadata?.let { defaultIndexMetadataService.getCustomIndexUUID(it) }
        // If the index metadata is null, the index is not in the cluster state. If the index metadata is not null, but
        // the cluster state index uuid differs from the one in the managed index config then the config is referring
        // to a different index which does not exist in the cluster. We need to check all of the extensions to confirm an index exists
        if (clusterStateIndexMetadata == null || clusterStateIndexUUID != managedIndexConfig.indexUuid) {
            // If the cluster state/default index type didn't have an index with a matching name and uuid combination, try all other index types
            val nonDefaultIndexTypes = indexMetadataProvider.services.keys.filter { it != DEFAULT_INDEX_TYPE }
            val multiTypeIndexNameToMetaData =
                indexMetadataProvider.getMultiTypeISMIndexMetadata(nonDefaultIndexTypes, listOf(managedIndexConfig.index))
            val someTypeMatchedUuid = multiTypeIndexNameToMetaData.values.any {
                it[managedIndexConfig.index]?.indexUuid == managedIndexConfig.indexUuid
            }
            // If no index types had an index with a matching name and uuid combination, return
            if (!someTypeMatchedUuid) {
                logger.warn("Failed to find IndexMetadata for ${managedIndexConfig.index}.")
                return
            }
        } else {
            val clusterStateMetadata = clusterStateIndexMetadata.getManagedIndexMetadata()
            val metadataCheck = checkMetadata(clusterStateMetadata, managedIndexMetaData, managedIndexConfig.indexUuid, logger)
            if (metadataCheck != MetadataCheck.SUCCESS) {
                logger.info("Skipping execution while metadata status is $metadataCheck")
                return
            }
        }

        // If policy or managedIndexMetaData is null then initialize
        val policy = managedIndexConfig.policy
        if (policy == null || managedIndexMetaData == null) {
            initManagedIndex(managedIndexConfig, managedIndexMetaData)
            return
        }

        // If the policy was completed or failed then return early and disable job so it stops scheduling work
        if (managedIndexMetaData.policyCompleted == true || managedIndexMetaData.isFailed) {
            disableManagedIndexConfig(managedIndexConfig)
            return
        }

        if (managedIndexMetaData.hasVersionConflict(managedIndexConfig)) {
            val info = mapOf("message" to "There is a version conflict between your previous execution and your managed index")
            val result = updateManagedIndexMetaData(
                managedIndexMetaData.copy(
                    policyRetryInfo = PolicyRetryInfoMetaData(true, 0),
                    info = info
                )
            )
            if (result.metadataSaved) {
                disableManagedIndexConfig(managedIndexConfig)
            }
            return
        }

        val state = policy.getStateToExecute(managedIndexMetaData)
        val action: Action? = state?.getActionToExecute(managedIndexMetaData, indexMetadataProvider)
        val stepContext = StepContext(
            managedIndexMetaData, clusterService, client, threadPool.threadContext, policy.user, scriptService, settings, jobContext.lockService
        )
        val step: Step? = action?.getStepToExecute(stepContext)
        val currentActionMetaData = action?.getUpdatedActionMetadata(managedIndexMetaData, state.name)

        // If Index State Management is disabled and the current step is not null and safe to disable on
        // then disable the job and return early
        if (!indexStateManagementEnabled && step != null && step.isSafeToDisableOn) {
            disableManagedIndexConfig(managedIndexConfig)
            return
        }

        if (action?.hasTimedOut(currentActionMetaData) == true) {
            val info = mapOf("message" to "Action timed out")
            logger.error("Action=${action.type} has timed out")
            val updated = updateManagedIndexMetaData(
                managedIndexMetaData
                    .copy(actionMetaData = currentActionMetaData?.copy(failed = true), info = info)
            )
            if (updated.metadataSaved) disableManagedIndexConfig(managedIndexConfig)
            return
        }

        if (managedIndexConfig.shouldChangePolicy(managedIndexMetaData, action)) {
            initChangePolicy(managedIndexConfig, managedIndexMetaData, action)
            return
        }

        val shouldBackOff = action?.shouldBackoff(currentActionMetaData, action.configRetry)
        if (shouldBackOff?.first == true) {
            // If we should back off then exit early.
            logger.info("Backoff for retrying. Remaining time ${shouldBackOff.second}")
            return
        }

        if (managedIndexMetaData.stepMetaData?.stepStatus == Step.StepStatus.STARTING) {
            val isIdempotent = step?.isIdempotent()
            logger.info("Previous execution failed to update step status, isIdempotent=$isIdempotent")
            if (isIdempotent != true) {
                val info = mapOf("message" to "Previous action was not able to update IndexMetaData.")
                val updated = updateManagedIndexMetaData(
                    managedIndexMetaData.copy(
                        policyRetryInfo = PolicyRetryInfoMetaData(true, 0), info = info
                    )
                )
                if (updated.metadataSaved) disableManagedIndexConfig(managedIndexConfig)
                return
            }
        }

        // If the extension from which action is registered is not enabled then the action will fail
        val actionExtensionName = ISMActionsParser.instance.customActionExtensionMap[action?.type]
        if (!extensionStatusChecker.isEnabled(actionExtensionName)) {
            val info = mapOf("message" to "Failed to execute action=${action?.type} as extension [$actionExtensionName] is not enabled.")
            val updated = updateManagedIndexMetaData(
                managedIndexMetaData.copy(
                    policyRetryInfo = PolicyRetryInfoMetaData(true, 0), info = info
                )
            )
            if (updated.metadataSaved) disableManagedIndexConfig(managedIndexConfig)
            return
        }

        // If this action is not allowed and the step to be executed is the first step in the action then we will fail
        // as this action has been removed from the AllowList, but if its not the first step we will let it finish as it's already inflight
        if (action?.isAllowed(allowList) == false && step != null && action.isFirstStep(step.name) && action.type != TransitionsAction.name) {
            val info = mapOf("message" to "Attempted to execute action=${action?.type} which is not allowed.")
            val updated = updateManagedIndexMetaData(
                managedIndexMetaData.copy(
                    policyRetryInfo = PolicyRetryInfoMetaData(true, 0), info = info
                )
            )
            if (updated.metadataSaved) disableManagedIndexConfig(managedIndexConfig)
            return
        }

        // If any of State, Action, Step components come back as null then we are moving to error in ManagedIndexMetaData
        val startingManagedIndexMetaData = managedIndexMetaData.getStartingManagedIndexMetaData(state, action, step)
        val updateResult = updateManagedIndexMetaData(startingManagedIndexMetaData)

        @Suppress("ComplexCondition", "MaxLineLength")
        if (updateResult.metadataSaved && state != null && action != null && step != null && currentActionMetaData != null) {
            if (validationServiceEnabled) {
                val validationResult = actionValidation.validate(action.type, stepContext.metadata.index)
                if (validationResult.validationStatus == Validate.ValidationStatus.RE_VALIDATING) {
                    logger.warn("Validation Status is: RE_VALIDATING. The action is {}, state is {}, step is {}.\", action.type, state.name, step.name")
                    publishErrorNotification(policy, managedIndexMetaData)
                    return
                }
                if (validationResult.validationStatus == Validate.ValidationStatus.FAILED) {
                    logger.warn("Validation Status is: FAILED. The action is {}, state is {}, step is {}.", action.type, state.name, step.name)
                    publishErrorNotification(policy, managedIndexMetaData)
                    disableManagedIndexConfig(managedIndexConfig)
                    return
                }
            }

            // Step null check is done in getStartingManagedIndexMetaData
            withClosableContext(
                IndexManagementSecurityContext(
                    managedIndexConfig.id, settings, threadPool.threadContext, managedIndexConfig.policy.user
                )
            ) {
                step.preExecute(logger, stepContext.getUpdatedContext(startingManagedIndexMetaData)).execute().postExecute(logger)
            }
            var executedManagedIndexMetaData = startingManagedIndexMetaData.getCompletedManagedIndexMetaData(action, step)

            if (executedManagedIndexMetaData.isFailed) {
                try {
                    // if the policy has no error_notification this will do nothing otherwise it will try to send the configured error message
                    publishErrorNotification(policy, executedManagedIndexMetaData)
                } catch (e: Exception) {
                    logger.error("Failed to publish error notification", e)
                    val errorMessage = e.message ?: "Failed to publish error notification"
                    val mutableInfo = executedManagedIndexMetaData.info?.toMutableMap() ?: mutableMapOf()
                    mutableInfo["errorNotificationFailure"] = errorMessage
                    executedManagedIndexMetaData = executedManagedIndexMetaData.copy(info = mutableInfo.toMap())
                }
            }

            if (executedManagedIndexMetaData.isSuccessfulDelete) {
                GlobalScope.launch(Dispatchers.IO + CoroutineName("ManagedIndexMetaData-AddHistory")) {
                    ismHistory.addManagedIndexMetaDataHistory(listOf(executedManagedIndexMetaData))
                }
                return
            }

            // If a custom action deletes some off-cluster index and has deleteIndexMetadataAfterFinish set to true,
            // then when the action successfully finishes, we will delete the managed index config and metadata. We do not
            // need to do this for the standard delete action as the coordinator picks up the index deletion and removes the config
            if (action.isFinishedSuccessfully(executedManagedIndexMetaData)) {
                if (action.deleteIndexMetadataAfterFinish()) {
                    deleteFromManagedIndex(managedIndexConfig, action.type)
                    return
                }
            }

            if (!updateManagedIndexMetaData(executedManagedIndexMetaData, updateResult).metadataSaved) {
                logger.error("Failed to update ManagedIndexMetaData after executing the Step : ${step.name}")
            }

            if (managedIndexConfig.hasDifferentJobInterval(jobInterval)) {
                updateJobInterval(managedIndexConfig, jobInterval)
            }
        }
    }

    private suspend fun initManagedIndex(managedIndexConfig: ManagedIndexConfig, managedIndexMetaData: ManagedIndexMetaData?) {
        var policy: Policy? = managedIndexConfig.policy
        val policyID = managedIndexConfig.changePolicy?.policyID ?: managedIndexConfig.policyID
        // If policy does not currently exist, we need to save the policy on the ManagedIndexConfig for the first time
        // or if a change policy exists then we will also execute the change as we are still in initialization phase
        if (policy == null || managedIndexConfig.changePolicy != null) {
            // Get the policy by the name unless a ChangePolicy exists then allow the change to happen during initialization
            policy = getPolicy(policyID)
            // Attempt to save the policy
            if (policy != null) {
                val saved = savePolicyToManagedIndexConfig(managedIndexConfig, policy)
                // If we failed to save the policy, don't initialize ManagedIndexMetaData
                if (!saved) return
            }
            // If we failed to get the policy then we will update the ManagedIndexMetaData with error info
        }

        // at this point we either successfully saved the policy or we failed to get the policy
        val updatedManagedIndexMetaData = if (policy == null) {
            getFailedInitializedManagedIndexMetaData(managedIndexMetaData, managedIndexConfig, policyID)
        } else {
            // Initializing ManagedIndexMetaData for the first time
            getInitializedManagedIndexMetaData(managedIndexMetaData, managedIndexConfig, policy)
        }

        updateManagedIndexMetaData(updatedManagedIndexMetaData, create = managedIndexMetaData == null)
    }

    @Suppress("ReturnCount", "BlockingMethodInNonBlockingContext")
    private suspend fun getPolicy(policyID: String): Policy? {
        try {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, policyID)
            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            if (!getResponse.isExists || getResponse.isSourceEmpty) {
                return null
            }

            val policySource = getResponse.sourceAsBytesRef
            // Intellij complains about createParser/parseWithType blocking because it sees they throw IOExceptions
            return withContext(Dispatchers.IO) {
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    policySource, XContentType.JSON
                )
                xcp.parseWithType(getResponse.id, getResponse.seqNo, getResponse.primaryTerm, Policy.Companion::parse)
            }
        } catch (e: Exception) {
            logger.error("Failed to get policy: $policyID", e)
            return null
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun disableManagedIndexConfig(managedIndexConfig: ManagedIndexConfig) {
        val updatedManagedIndexConfig = managedIndexConfig.copy(enabled = false, jobEnabledTime = null)
        val indexRequest = updateDisableManagedIndexRequest(updatedManagedIndexConfig.indexUuid)
        try {
            val indexResponse: UpdateResponse = client.suspendUntil { update(indexRequest, it) }
            if (indexResponse.status() != RestStatus.OK) {
                logger.error("Failed to disable ManagedIndexConfig(${managedIndexConfig.index}) Error : indexResponse.status()")
            }
        } catch (e: Exception) {
            logger.error("Failed to disable ManagedIndexConfig(${managedIndexConfig.index})", e)
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun savePolicyToManagedIndexConfig(managedIndexConfig: ManagedIndexConfig, policy: Policy): Boolean {
        val updatedManagedIndexConfig = managedIndexConfig.copy(
            policyID = policy.id, policy = policy,
            policySeqNo = policy.seqNo, policyPrimaryTerm = policy.primaryTerm, changePolicy = null
        )
        val indexRequest = managedIndexConfigIndexRequest(updatedManagedIndexConfig)
        var savedPolicy = false
        try {
            savePolicyRetryPolicy.retry(logger) {
                val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
                savedPolicy = indexResponse.status() == RestStatus.OK
            }
        } catch (e: VersionConflictEngineException) {
            logger.error("Failed to save policy(${policy.id}) to ManagedIndexConfig(${managedIndexConfig.index}). ${e.message}")
        } catch (e: Exception) {
            logger.error("Failed to save policy(${policy.id}) to ManagedIndexConfig(${managedIndexConfig.index})", e)
        }
        return savedPolicy
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun updateJobInterval(managedIndexConfig: ManagedIndexConfig, jobInterval: Int) {
        try {
            val updatedManagedIndexConfig = managedIndexConfig
                .copy(jobSchedule = IntervalSchedule(getIntervalStartTime(managedIndexConfig), jobInterval, ChronoUnit.MINUTES))
            val indexRequest = managedIndexConfigIndexRequest(updatedManagedIndexConfig)
            val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            if (indexResponse.status() != RestStatus.OK) {
                logger.error("Failed to update ManagedIndexConfig(${managedIndexConfig.index}) job interval")
            }
        } catch (e: VersionConflictEngineException) {
            logger.error("Failed to update ManagedIndexConfig(${managedIndexConfig.index}) job interval. ${e.message}")
        } catch (e: Exception) {
            logger.error("Failed to update ManagedIndexConfig(${managedIndexConfig.index}) job interval", e)
        }
    }

    private suspend fun getFailedInitializedManagedIndexMetaData(
        managedIndexMetaData: ManagedIndexMetaData?,
        managedIndexConfig: ManagedIndexConfig,
        policyID: String
    ): ManagedIndexMetaData {
        // we either haven't initialized any metadata yet or we have already initialized metadata but still have no policy
        return managedIndexMetaData?.copy(
            policyRetryInfo = PolicyRetryInfoMetaData(failed = true, consumedRetries = 0),
            info = mapOf("message" to "Fail to load policy: $policyID")
        ) ?: ManagedIndexMetaData(
            index = managedIndexConfig.index,
            indexUuid = managedIndexConfig.indexUuid,
            policyID = policyID,
            policySeqNo = null,
            policyPrimaryTerm = null,
            policyCompleted = false,
            rolledOver = false,
            indexCreationDate = getIndexCreationDate(managedIndexConfig),
            transitionTo = null,
            stateMetaData = null,
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(failed = true, consumedRetries = 0),
            info = mapOf("message" to "Fail to load policy: $policyID")
        )
    }

    @Suppress("ComplexMethod")
    private suspend fun getInitializedManagedIndexMetaData(
        managedIndexMetaData: ManagedIndexMetaData?,
        managedIndexConfig: ManagedIndexConfig,
        policy: Policy
    ): ManagedIndexMetaData {
        val state = managedIndexConfig.changePolicy?.state ?: policy.defaultState
        val stateMetaData = StateMetaData(state, Instant.now().toEpochMilli())

        return when {
            managedIndexMetaData == null -> ManagedIndexMetaData(
                index = managedIndexConfig.index,
                indexUuid = managedIndexConfig.indexUuid,
                policyID = policy.id,
                policySeqNo = policy.seqNo,
                policyPrimaryTerm = policy.primaryTerm,
                policyCompleted = false,
                rolledOver = false,
                indexCreationDate = getIndexCreationDate(managedIndexConfig),
                transitionTo = null,
                stateMetaData = stateMetaData,
                actionMetaData = null,
                stepMetaData = null,
                policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
                info = mapOf("message" to "Successfully initialized policy: ${policy.id}")
            )
            managedIndexMetaData.policySeqNo == null || managedIndexMetaData.policyPrimaryTerm == null ->
                // If there is seqNo and PrimaryTerm it is first time populating Policy.
                managedIndexMetaData.copy(
                    policyID = policy.id,
                    policySeqNo = policy.seqNo,
                    policyPrimaryTerm = policy.primaryTerm,
                    stateMetaData = stateMetaData,
                    policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
                    info = mapOf("message" to "Successfully initialized policy: ${policy.id}")
                )
            // this is an edge case where a user deletes the job config or index and we already have a policySeqNo/primaryTerm
            // in the metadata, in this case we just want to say we successfully initialized the policy again but we will not
            // modify the state, action, etc. so it can resume where it left off
            managedIndexMetaData.policySeqNo == policy.seqNo &&
                managedIndexMetaData.policyPrimaryTerm == policy.primaryTerm &&
                managedIndexMetaData.policyID == policy.id ->
                // If existing PolicySeqNo and PolicyPrimaryTerm is equal to cached Policy then no issue.
                managedIndexMetaData.copy(
                    policyRetryInfo = PolicyRetryInfoMetaData(failed = false, consumedRetries = 0),
                    info = mapOf("message" to "Successfully initialized policy: ${policy.id}")
                )
            else ->
                // else this means we either tried to load a policy with a different id, seqno, or primaryterm from what is
                // in the metadata and we cannot guarantee it will work with the current state in managedIndexMetaData
                managedIndexMetaData.copy(
                    policyRetryInfo = PolicyRetryInfoMetaData(failed = true, consumedRetries = 0),
                    info = mapOf(
                        "message" to "Fail to load policy: ${policy.id} with " +
                            "seqNo ${policy.seqNo} and primaryTerm ${policy.primaryTerm} as it" +
                            " does not match what's in the metadata [policyID=${managedIndexMetaData.policyID}," +
                            " policySeqNo=${managedIndexMetaData.policySeqNo}, policyPrimaryTerm=${managedIndexMetaData.policyPrimaryTerm}]"
                    )
                )
        }
    }

    /**
     * update metadata in config index, and save metadata in history after update
     * this can be called 2 times in one job run, so need to save seqNo & primeTerm
     */
    private suspend fun updateManagedIndexMetaData(
        managedIndexMetaData: ManagedIndexMetaData,
        lastUpdateResult: UpdateMetadataResult? = null,
        create: Boolean = false
    ): UpdateMetadataResult {
        var result = UpdateMetadataResult()
        if (!imIndices.attemptUpdateConfigIndexMapping()) {
            logger.error("Failed to update config index mapping.")
            return result
        }

        var metadata: ManagedIndexMetaData = managedIndexMetaData
        if (lastUpdateResult != null) {
            metadata = managedIndexMetaData.copy(seqNo = lastUpdateResult.seqNo, primaryTerm = lastUpdateResult.primaryTerm)
        }

        val indexRequest = managedIndexMetadataIndexRequest(metadata, create = create)
        try {
            updateMetaDataRetryPolicy.retry(logger) {
                val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
                val metadataSaved = indexResponse.status() == RestStatus.OK ||
                    indexResponse.status() == RestStatus.CREATED
                result = UpdateMetadataResult(metadataSaved, indexResponse.seqNo, indexResponse.primaryTerm)
            }

            GlobalScope.launch(Dispatchers.IO + CoroutineName("ManagedIndexMetaData-AddHistory")) {
                ismHistory.addManagedIndexMetaDataHistory(listOf(metadata))
            }
        } catch (e: VersionConflictEngineException) {
            logger.error(
                "There was VersionConflictEngineException trying to update the metadata for " +
                    "${managedIndexMetaData.index}. Message: ${e.message}",
                e
            )
        } catch (e: Exception) {
            logger.error("Failed to save ManagedIndexMetaData for [index=${managedIndexMetaData.index}]", e)
        }
        return result
    }

    data class UpdateMetadataResult(
        val metadataSaved: Boolean = false,
        val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
        val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
    )

    /**
     * Initializes the change policy process where we will get the policy using the change policy's policyID, update the [ManagedIndexMetaData]
     * to reflect the new policy, and save the new policy to the [ManagedIndexConfig] while resetting the change policy to null
     */
    @Suppress("ReturnCount", "ComplexMethod")
    private suspend fun initChangePolicy(
        managedIndexConfig: ManagedIndexConfig,
        managedIndexMetaData: ManagedIndexMetaData,
        actionToExecute: Action?
    ) {

        // should never happen since we only call this if there is a changePolicy, but we'll do it to make changePolicy non null
        val changePolicy = managedIndexConfig.changePolicy
        if (changePolicy == null) {
            logger.debug("initChangePolicy was called with a null ChangePolicy, ManagedIndexConfig: $managedIndexConfig")
            return
        }

        // get the policy we'll attempt to change to
        val policy = getPolicy(changePolicy.policyID)

        // update the ManagedIndexMetaData with new information
        val updatedManagedIndexMetaData = if (policy == null) {
            managedIndexMetaData.copy(
                info = mapOf("message" to "Failed to load change policy: ${changePolicy.policyID}"),
                policyRetryInfo = PolicyRetryInfoMetaData(failed = true, consumedRetries = 0)
            )
        } else {
            // if the action to execute is transition then set the actionMetaData to a new transition metadata to reflect we are
            // in transition (in case we triggered change policy from entering transition) or to reflect this is a new policy transition phase
            val newTransitionMetaData = ActionMetaData(
                TransitionsAction.name, Instant.now().toEpochMilli(), -1,
                false, 0, 0, null
            )
            val actionMetaData = if (actionToExecute?.type == TransitionsAction.name) {
                newTransitionMetaData
            } else {
                managedIndexMetaData.actionMetaData
            }
            managedIndexMetaData.copy(
                info = mapOf("message" to "Attempting to change policy to ${policy.id}"),
                transitionTo = changePolicy.state,
                actionMetaData = actionMetaData,
                stepMetaData = null,
                policyCompleted = false,
                policySeqNo = policy.seqNo,
                policyPrimaryTerm = policy.primaryTerm,
                policyID = policy.id
            )
        }

        // check if the safe flag was set by the Change Policy REST API, if it was then do a second validation
        // before allowing a change to happen
        if (changePolicy.isSafe) {
            // if policy is null then we are only updating error information in metadata so its fine to continue
            if (policy != null) {
                // current policy being null should never happen as we have a check at the top of runner
                // if it is unsafe to change then we set safe back to false so we don't keep doing this check every execution
                if (managedIndexConfig.policy?.isSafeToChange(managedIndexMetaData.stateMetaData?.name, policy, changePolicy) != true) {
                    updateManagedIndexConfig(managedIndexConfig.copy(changePolicy = managedIndexConfig.changePolicy.copy(isSafe = false)))
                    return
                }
            }
        }

        /*
        * Try to update the ManagedIndexMetaData in cluster state, we need to do this first before updating the
        * ManagedIndexConfig because if this fails we can fail early and still retry this whole process on the next
        * execution whereas if we do the update to ManagedIndexConfig first we lose the ChangePolicy on the job and
        * could fail to update the ManagedIndexMetaData which would put us in a bad state
        * */
        val updated = updateManagedIndexMetaData(updatedManagedIndexMetaData)

        if (!updated.metadataSaved || policy == null) return

        // Change the policy and user stored on the job from changePolicy, this will also set the changePolicy to null on the job
        savePolicyToManagedIndexConfig(managedIndexConfig, policy.copy(user = changePolicy.user))
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun updateManagedIndexConfig(updatedManagedIndexConfig: ManagedIndexConfig) {
        try {
            val indexRequest = managedIndexConfigIndexRequest(updatedManagedIndexConfig)
            val indexResponse: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            if (indexResponse.status() != RestStatus.OK) {
                logger.error("Failed to update ManagedIndexConfig(${updatedManagedIndexConfig.index})")
            }
        } catch (e: VersionConflictEngineException) {
            logger.error("Failed to update ManagedIndexConfig(${updatedManagedIndexConfig.index}). ${e.message}")
        } catch (e: Exception) {
            logger.error("Failed to update ManagedIndexConfig(${updatedManagedIndexConfig.index})", e)
        }
    }

    private suspend fun publishErrorNotification(policy: Policy, managedIndexMetaData: ManagedIndexMetaData) {
        policy.errorNotification?.run {
            errorNotificationRetryPolicy.retry(logger) {
                val compiledMessage = compileTemplate(messageTemplate, managedIndexMetaData)
                destination?.buildLegacyBaseMessage(null, compiledMessage)?.publishLegacyNotification(client)
                channel?.sendNotification(client, ErrorNotification.CHANNEL_TITLE, managedIndexMetaData, compiledMessage, policy.user)
            }
        }
    }

    private fun compileTemplate(template: Script, managedIndexMetaData: ManagedIndexMetaData): String {
        return try {
            scriptService.compile(template, TemplateScript.CONTEXT)
                .newInstance(template.params + mapOf("ctx" to managedIndexMetaData.convertToMap()))
                .execute()
        } catch (e: Exception) {
            val message = "There was an error compiling mustache template"
            logger.error(message, e)
            e.message ?: message
        }
    }

    private fun clusterIsRed(): Boolean = ClusterStateHealth(clusterService.state()).status == ClusterHealthStatus.RED

    private suspend fun getIndexMetadata(index: String): IndexMetadata? {
        var indexMetaData: IndexMetadata? = null
        try {
            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(index)
                .metadata(true)
                .local(false)
                .indicesOptions(IndicesOptions.strictExpand())

            val response: ClusterStateResponse = client.admin().cluster().suspendUntil { state(clusterStateRequest, it) }

            indexMetaData = response.state.metadata.indices.firstOrNull()?.value
        } catch (e: Exception) {
            logger.error("Failed to get IndexMetaData from cluster manager cluster state for index=$index", e)
        }

        return indexMetaData
    }

    // TODO: This is a hacky solution to get the current start time off the job interval as job-scheduler currently does not
    //  make this public, long term solution is to make the changes in job-scheduler, cherry-pick back into ISM supported versions and
    //  republish job-scheduler spi to maven, in the interim we will parse the current interval start time
    private suspend fun getIntervalStartTime(managedIndexConfig: ManagedIndexConfig): Instant {
        return withContext(Dispatchers.IO) {
            val intervalJsonString = managedIndexConfig.schedule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
            val xcp = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, intervalJsonString)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp) // start of schedule block
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp) // "interval"
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp) // start of interval block
            var startTime: Long? = null
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    "start_time" -> startTime = xcp.longValue()
                }
            }
            Instant.ofEpochMilli(requireNotNull(startTime))
        }
    }

    /**
     * Get the index creation date for the first time to cache it on the ManagedIndexMetadata
     */
    @Suppress("ReturnCount")
    private suspend fun getIndexCreationDate(managedIndexConfig: ManagedIndexConfig): Long? {
        try {
            val multiTypeIndexNameToMetaData = indexMetadataProvider.getMultiTypeISMIndexMetadata(indexNames = listOf(managedIndexConfig.index))
            // the managedIndexConfig.indexUuid should be unique across all index types
            val indexCreationDate = multiTypeIndexNameToMetaData.values.firstOrNull {
                it[managedIndexConfig.index]?.indexUuid == managedIndexConfig.indexUuid
            }?.get(managedIndexConfig.index)?.indexCreationDate
            return indexCreationDate
        } catch (e: Exception) {
            logger.error("Failed to get the index creation date", e)
        }
        return null
    }

    /**
     * Deletes a managedIndexConfig and its managedIndexMetadata. Used after an action has successfully completely
     * and the action has deleteIndexMetadataAfterFinish set to true. This should only be set to true for custom
     * actions added by extensions, as when indices in the cluster are deleted,the deletion should get picked up
     * and the ism config metadata for the index will be removed, but if the index is not in the cluster, as it
     * might be for some other index type, we have to do this manually
     */
    private suspend fun deleteFromManagedIndex(managedIndexConfig: ManagedIndexConfig, actionType: String) {
        try {
            val bulkRequest = BulkRequest()
                .add(deleteManagedIndexRequest(managedIndexConfig.indexUuid))
                .add(deleteManagedIndexMetadataRequest(managedIndexConfig.indexUuid))

            val bulkResponse: BulkResponse = client.suspendUntil { bulk(bulkRequest, it) }
            for (bulkItemResponse in bulkResponse) {
                if (bulkItemResponse.isFailed) {
                    logger.warn(
                        "Failed to delete managed index job/metadata [id=${bulkItemResponse.id}] for ${managedIndexConfig.index}" +
                            " after a successful $actionType [result=${bulkItemResponse.failureMessage}]"
                    )
                }
            }
        } catch (e: Exception) {
            logger.warn("Failed to delete managed index job for for ${managedIndexConfig.index} after a successful $actionType", e)
        }
    }
}
