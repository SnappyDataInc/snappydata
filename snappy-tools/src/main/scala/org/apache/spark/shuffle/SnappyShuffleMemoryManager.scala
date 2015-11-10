package org.apache.spark.shuffle

import org.apache.spark.storage.SnappyMemoryUtils

/**
 * Created by shirishd on 15/10/15.
 */

private[spark] class SnappyShuffleMemoryManager protected(override val maxMemory: Long,
    override val pageSizeBytes: Long) extends ShuffleMemoryManager(maxMemory, pageSizeBytes) {

  override def tryToAcquire(numBytes: Long): Long = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to tryToAcquire
    if (!taskMemory.contains(taskAttemptId)) {
      taskMemory(taskAttemptId) = 0L
      notifyAll() // Will later cause waiting tasks to wake up and check numThreads again
    }

    if (SnappyMemoryUtils.isCriticalUp) {
      logInfo(s"Will not store $numBytes bytes as CRITICAL UP event is detected")
      0
    } else {
      super.tryToAcquire(numBytes)
    }
  }
}

