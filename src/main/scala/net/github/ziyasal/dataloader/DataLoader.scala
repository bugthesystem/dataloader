package net.github.ziyasal.dataloader

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

// Wait `dotty` for union types :(
// V | Exception

class DataLoader[K: ClassTag, V](
    private var batchLoadFn: (Array[K]) => Option[Promise[Array[V]]],
    private var options: Options[K, V] = new Options[K, V]()
) {

  private var promiseCache: CacheMap[K, Promise[V]] = _
  private var queue: Queue[LoaderQueueItem[K, V]] = Queue[LoaderQueueItem[K, V]]()

  promiseCache = options.cacheMap

  def load(key: K): Future[V] = {

    // Determine options
    val shouldBatch = options.batch
    val shouldCache = options.cache
    val cacheKeyFn = options.cacheKeyFn
    val cacheKey = cacheKeyFn match {
      case Some(x) => x(key)
      case None    => key
    }

    // If caching and there is a cache-hit, return cached Promise.
    if (shouldCache) {
      val cachedPromiseOpt: Option[Promise[V]] = promiseCache.get(cacheKey)
      cachedPromiseOpt match {
        case Some(cachedPromise) => return cachedPromise.future
        case _                   => {}
      }
    }

    val promise: Promise[V] = Promise()

    futurify(promise, key, shouldBatch)

    // If caching, cache this promise.
    if (shouldCache) {
      promiseCache.set(cacheKey, promise)
    }

    promise.future
  }

  private def futurify(promise: Promise[V], key: K, shouldBatch: Boolean) = {
    // Otherwise, produce a new Promise for this value.
    val _: Future[Unit] = Future {

      // Enqueue this Promise to be dispatched.
      queue = queue.enqueue(LoaderQueueItem(
        key = key,
        resolve = (value: V) => promise.success(value),
        reject = (exc: Exception) => promise.failure(exc)
      ))

      // Determine if a dispatch of this queue should be scheduled.
      // A single dispatch should be scheduled per queue at the time when the
      // queue changes from "empty" to "full".
      if (queue.length == 1) {
        if (shouldBatch) {
          // If batching, schedule a task to dispatch the queue.
          dispatchQueue(this)
        } else {
          // Otherwise dispatch the (queue of one) immediately.
          dispatchQueue(this)
        }
      }
    }
  }

  def loadMany(keys: Array[K]): Future[Seq[V]] = {

    val futures: Seq[Future[V]] = keys.map(key => this.load(key))
    Future.sequence(futures)
  }

  def clear(key: K): DataLoader[K, V] = {
    val cacheKeyFn = options.cacheKeyFn
    val cacheKey = cacheKeyFn match {
      case Some(x) => x(key)
      case None    => key
    }
    promiseCache.clear(cacheKey)

    this
  }

  def clearAll(): DataLoader[K, V] = {
    promiseCache.clearAll()

    this
  }

  def prime(key: K, value: V): DataLoader[K, V] = {

    val cacheKeyFn = options.cacheKeyFn
    val cacheKey = cacheKeyFn match {
      case Some(x) => x(key)
      case None    => key
    }

    // Only add the key if it does not already exist.
    val maybePromise = promiseCache.get(cacheKey)
    maybePromise match {
      case None => {
        // Cache a rejected promise if the value is an Error, in order to match
        // the behavior of load(key).
        val promise: Promise[V] = value match {
          case e: Exception => Promise[V]().failure(e)
          case _            => Promise[V]().success(value)
        }

        promiseCache.set(cacheKey, promise)
      }
      case _ => {}
    }

    this
  }

  def dispatchQueue(loader: DataLoader[K, V]) {
    // Take the current loader queue, replacing it with an empty queue.
    val queue = loader.queue
    loader.queue = Queue[LoaderQueueItem[K, V]]() //TODO:

    // If a maxBatchSize was provided and the queue is longer, then segment the
    // queue into multiple batches, otherwise treat the queue as a single batch.
    val maxBatchSizeOpt = loader.options.maxBatchSize

    maxBatchSizeOpt match {
      case Some(maxBatchSize) => {

        if (maxBatchSize > 0 && maxBatchSize < queue.length) {
          for (i <- 0 until queue.length / maxBatchSize) {
            dispatchQueueBatch(
              loader,
              queue.slice(i * maxBatchSize, (i + 1) * maxBatchSize)
            )
          }
        } else {
          dispatchQueueBatch(loader, queue)
        }

      }
      case None => {
        dispatchQueueBatch(loader, queue)
      }
    }
  }

  def dispatchQueueBatch(
    loader: DataLoader[K, V],
    queue: Queue[LoaderQueueItem[K, V]]
  ) {
    // Collect all keys to be loaded in this dispatch
    val keys = queue.map(x => x.key).toArray[K]

    // Call the provided batchLoadFn for this loader with the loader queue's keys.
    val batchLoadFn = loader.batchLoadFn
    val batchPromiseOpt = batchLoadFn(keys)

    batchPromiseOpt match {
      case Some(batchPromise) => {
        // Await the resolution of the call to batchLoadFn.
        batchPromise.future onComplete {
          case Success(values: Array[V]) => {

            if (values.length != keys.length) {
              throw new Exception(
                "DataLoader must be constructed with a function which accepts" +
                  "Array[Key] and returns Promise[Array[Value]] but the function did" +
                  "not return a Promise of an Array of the same length as the Array" +
                  s"of keys. \n\nKeys:\n${keys} \n\nValues:\n${values}"
              )
            }
            processQueueInBatch(queue, values)
          }
          case Failure(ex: Throwable) => {
            ex match {
              case e: Exception => failedDispatch(loader, queue, e)
            }
          }
        }
      }
      case None => {
        // Assert the expected response from batchLoadFn
        failedDispatch(loader, queue, new Exception(
          "DataLoader must be constructed with a function which accepts " +
            "Array[Key] and returns Promise[Array[Value]], but the function did " +
            s"not return a Promise: ${batchPromiseOpt.toString}."
        ))
      }
    }
  }

  private def processQueueInBatch(queue: Queue[LoaderQueueItem[K, V]], values: Array[V]) = {
    // Step through the values, resolving or rejecting each Promise in the
    // loaded queue.
    queue.indices.foreach { index =>
      {

        val q = queue(index)

        val value = values(index)
        value match {
          case e: Exception =>
            q.reject(e)
          case _ =>
            q.resolve(value)
        }
      }
    }
  }

  // Private: do not cache individual loads if the entire batch dispatch fails,
  // but still reject each request so they do not hang.
  private def failedDispatch(
    loader: DataLoader[K, V],
    queue: Queue[LoaderQueueItem[K, V]],
    error: Exception
  ) {
    queue.foreach { q =>
      {
        loader.clear(q.key)
        q.reject(error)
      }

    }

  }
}
