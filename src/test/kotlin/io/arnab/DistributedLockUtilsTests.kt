package io.arnab

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.fest.assertions.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

object DistributedLockUtilsTests {
    private val lockId = UUIDUtils.uuidBase32("TEST")
    private val anotherLockId = UUIDUtils.uuidBase32("TEST")

    @Test
    fun `should be able to acquire and release lock`() {
        val lockAcquired = runBlocking { DistributedLockUtils.acquireLock(lockId, 12) }
        assertThat(lockAcquired).isTrue
        runBlocking { DistributedLockUtils.releaseLock(lockId) }
    }

    @Test
    fun `should prevent others from acquiring lock once lock is already acquired`() {
        val lockAcquired = runBlocking { DistributedLockUtils.acquireLock(lockId, 12) }
        assertThat(lockAcquired).isTrue
        val lockAcquiredSecond = runBlocking { DistributedLockUtils.acquireLock(lockId) }
        assertThat(lockAcquiredSecond).isFalse
        runBlocking { DistributedLockUtils.releaseLock(lockId) }
    }


    @Test
    fun `release lock behaviour should be idempotent`() {
        val lockAcquired = runBlocking { DistributedLockUtils.acquireLock(lockId, 12) }
        assertThat(lockAcquired).isTrue
        runBlocking { DistributedLockUtils.releaseLock(lockId) }
        runBlocking { DistributedLockUtils.releaseLock(lockId) }
    }

    @Test
    fun `should be able to acquire lock once lease expires`() {
        val lockAcquired = runBlocking { DistributedLockUtils.acquireLock(lockId, 1) }
        assertThat(lockAcquired).isTrue
        runBlocking { delay(2000) }
        val lockAcquiredAgain = runBlocking { DistributedLockUtils.acquireLock(lockId, 1) }
        assertThat(lockAcquiredAgain).isTrue
        runBlocking { DistributedLockUtils.releaseLock(lockId) }
    }

    @Test
    fun `should execute block with lock`() {
        val ans = runBlocking {
            DistributedLockUtils.executeWithLock(lockId) { 1 + 8 }
        }
        assertThat(ans).isEqualTo(9)
    }

    @Test
    fun `should not execute block if lock acquisition fails`() {
        runBlocking { DistributedLockUtils.acquireLock(lockId, 6) }

        val thrown = Assertions.assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                DistributedLockUtils.executeWithLock(lockId, retryLockAttempts = 0) { 1 + 8 }
            }
        }
        assertThat(thrown.message).isEqualTo("failed to acquire lock after 0 attempts")
        runBlocking { DistributedLockUtils.releaseLock(lockId) }
    }

    @Test
    fun `should execute block if already acquired lock lease expires`() {
        val acquired = runBlocking { DistributedLockUtils.acquireLock(lockId, 1) }
        assertThat(acquired).isTrue
        val ans = runBlocking {
            DistributedLockUtils.executeWithLock(lockId) { 1 + 8 }
        }
        runBlocking { DistributedLockUtils.releaseLock(lockId) }
        assertThat(ans).isEqualTo(9)
    }

    @Test
    fun `should execute block with locks`() {
        val ans = runBlocking {
            DistributedLockUtils.executeWithLocks(setOf(lockId, anotherLockId)) { 1 + 8 }
        }
        assertThat(ans).isEqualTo(9)
    }

    @Test
    fun `should execute block if already acquired locks' lease expires`() {
        val acquired = runBlocking { DistributedLockUtils.acquireLock(lockId, 2) }
        assertThat(acquired).isTrue
        val anotherAcquired = runBlocking { DistributedLockUtils.acquireLock(anotherLockId, 1) }
        assertThat(anotherAcquired).isTrue
        val ans = runBlocking {
            DistributedLockUtils.executeWithLocks(setOf(lockId, anotherLockId)) { 1 + 8 }
        }
        runBlocking {
            DistributedLockUtils.releaseLock(lockId)
            DistributedLockUtils.releaseLock(anotherLockId)
        }
        assertThat(ans).isEqualTo(9)
    }
}