package io.arnab

import kotlinx.coroutines.delay

object DistributedLockUtils {
    data class Lock(val id: String, val ttl: Int)

    private val mongoDbDao by lazy {
        MongoDBDao(
            configKey = DBConfig.getDBConfigKey() ?: Constants.DB_CONFIG_KEY,
            databaseName = DBConfig.getDBName() ?: Constants.LOCK_DB_NAME,
            collectionName = DBConfig.getDBCollectionName() ?: Constants.LOCK_COLLECTION
        )
    }

    private const val MINIMUM_LEASE_DURATION_IN_SECONDS = 1
    private const val MAXIMUM_LEASE_DURATION_IN_SECONDS = 20 * 60
    private const val DEFAULT_LEASE_DURATION_IN_SECONDS = 10
    private const val DEFAULT_RETRY_IN_MILLISECONDS = 800
    private const val DEFAULT_LOCK_ATTEMPTS = 5
    private val DISALLOWED_CHARS = setOf('#', '?', '\\', '/')

    suspend fun <T> executeWithLock(
        lockId: String,
        leaseDurationInSeconds: Int = DEFAULT_LEASE_DURATION_IN_SECONDS,
        retryLockAttempts: Int = DEFAULT_LOCK_ATTEMPTS,
        retryDelayInMillis: Int = DEFAULT_RETRY_IN_MILLISECONDS,
        block: suspend () -> T,
    ): T {
        val acquired = acquireLockWithRetries(lockId, leaseDurationInSeconds, retryLockAttempts, retryDelayInMillis)
        require(acquired) { "failed to acquire lock after $retryLockAttempts attempts" }

        try {
            return block()
        } finally {
            releaseLock(lockId)
        }
    }

    suspend fun <T> executeWithLocks(
        lockIds: Set<String>,
        leaseDurationInSeconds: Int = DEFAULT_LEASE_DURATION_IN_SECONDS,
        retryLockAttempts: Int = DEFAULT_LOCK_ATTEMPTS,
        retryDelayInMillis: Int = DEFAULT_RETRY_IN_MILLISECONDS,
        block: suspend () -> T,
    ): T {
        val acquiredForAll = try {
            lockIds.parallelMap { lockId ->
                acquireLockWithRetries(lockId, leaseDurationInSeconds, retryLockAttempts, retryDelayInMillis)
            }.all { it }
        } catch (e: Exception) {
            lockIds.forEach { releaseLock(it) }
            throw e
        }
        require(acquiredForAll) { "failed to acquire locks ${lockIds.joinToString(",")}" }
        try {
            return block()
        } finally {
            lockIds.forEach { releaseLock(it) }
        }
    }

    private suspend fun acquireLockWithRetries(
        lockId: String,
        leaseDurationInSeconds: Int = DEFAULT_LEASE_DURATION_IN_SECONDS,
        retryLockAttempts: Int = DEFAULT_LOCK_ATTEMPTS,
        retryDelayInMillis: Int = DEFAULT_RETRY_IN_MILLISECONDS
    ): Boolean {
        validate(lockId, leaseDurationInSeconds)
        var acquired = acquireLock(lockId, leaseDurationInSeconds)
        if (!acquired) {
            acquired = tryLockAcquisition(lockId, leaseDurationInSeconds, retryLockAttempts, retryDelayInMillis)
        }

        return acquired
    }

    private fun validate(id: String, lease: Int) {
        require(lease <= MAXIMUM_LEASE_DURATION_IN_SECONDS) {
            "Lease duration should be less than or equal to $MAXIMUM_LEASE_DURATION_IN_SECONDS seconds"
        }
        require(lease >= MINIMUM_LEASE_DURATION_IN_SECONDS) {
            "Lease duration should be at least $MINIMUM_LEASE_DURATION_IN_SECONDS seconds"
        }
        require(id.isNotBlank()) { "ID cannot be blank" }
        require(id.length <= 200) { "ID length should be less than or equal to 200 chars" }
        require(id.none { DISALLOWED_CHARS.contains(it) }) { "Following chars $DISALLOWED_CHARS cannot be present in id" }
    }

    suspend fun acquireLock(id: String, leaseDurationInSeconds: Int = DEFAULT_LEASE_DURATION_IN_SECONDS): Boolean {
        validate(id, leaseDurationInSeconds)
        val lock = Lock(id, leaseDurationInSeconds)
        return try {
            // create item in mongoDB
            mongoDbDao.mongoCollection.createItem(lock, PartitionKey(id)).toFuture().await()
            true
        } catch (e: Exception) {
            if (e.statusCode == HttpConstants.StatusCodes.CONFLICT) false else throw e
        }
    }

    private suspend fun tryLockAcquisition(
        lockId: String,
        leaseDurationInSeconds: Int,
        totalAttempts: Int,
        delayInMillis: Int,
    ): Boolean {
        var acquired = false
        var attempt = 1
        while (acquired.not() && attempt++ < totalAttempts) {
            acquired = acquireLock(lockId, leaseDurationInSeconds)
            if (!acquired) {
                delay(delayInMillis.toLong())
            }
        }
        return acquired
    }

    suspend fun releaseLock(id: String) {
        try {
            // delete item in mongodb
            mongoDbDao.mongoCollection.deleteItem(id, PartitionKey(id)).toFuture().await()
        } catch (e: Exception) {
            if (e.statusCode == HttpConstants.StatusCodes.NOTFOUND) return else throw e
        }
    }
}