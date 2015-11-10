package org.apache.spark.storage

import org.apache.spark.network.BlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.{MapOutputTracker, SecurityManager, SparkConf}

/**
 * Created by shirishd on 12/10/15.
 */

private[spark] class SnappyBlockManager(
    executorId: String,
    rpcEnv: RpcEnv,
    override val master: BlockManagerMaster,
    defaultSerializer: Serializer,
    override val conf: SparkConf,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int)
    extends BlockManager(executorId, rpcEnv, master, defaultSerializer, conf, mapOutputTracker,
      shuffleManager, blockTransferService, securityManager, numUsableCores) {

    override private[spark] val memoryStore = new SnappyMemoryStore(this, BlockManager.getMaxMemory(conf))
}

