package net.github.ziyasal.dataloader

import scala.concurrent.Promise

class Options[K, V](
    var maxBatchSize: Option[Int] = None,
    var cacheKeyFn: Option[(K) => K] = None,
    var batch: Boolean = true,
    var cache: Boolean = true,
    var cacheMap: CacheMap[K, Promise[V]] = new CacheMap[K, Promise[V]]
) {

}
