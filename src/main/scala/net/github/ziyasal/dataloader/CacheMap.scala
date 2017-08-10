package net.github.ziyasal.dataloader

import scala.collection.mutable

class CacheMap[K, V] {

  private val promiseCache: mutable.Map[K, V] = mutable.Map[K, V]()

  def get(key: K): Option[V] = {

    if (promiseCache.contains(key)) {
      Some(promiseCache(key))
    }

    None
  }

  def has(key: K): Boolean = {
    promiseCache.contains(key)
  }

  def set(key: K, value: V): CacheMap[K, V] = {

    promiseCache(key) = value

    this
  }

  def clear(key: K): CacheMap[K, V] = {

    promiseCache.remove(key)

    this
  }

  def clearAll(): CacheMap[K, V] = {

    promiseCache.clear()

    this
  }

}
