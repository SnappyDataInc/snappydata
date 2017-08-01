package org.apache.spark.executor

import com.gemstone.gemfire.cache.CacheClosedException
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.{SparkCallBackFactory, SnappySparkCallback}

/**
 * Created by sachin on 1/8/17.
 */
object SnappySparkCallbacksImpl extends SnappySparkCallback {
  override def checkCacheClosing(t: Throwable): Boolean = {

    try {
      Misc.checkIfCacheClosing(t)
    } catch {
      case ex: CacheClosedException => true
      case _ => false
    }
    return false;
  }
}

trait SparkCallBack extends Serializable {
  // Register callback
  SparkCallBackFactory.setSnappySparkCallback(SnappySparkCallbacksImpl)
}
