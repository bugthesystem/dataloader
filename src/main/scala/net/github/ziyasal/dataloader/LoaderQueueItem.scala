package net.github.ziyasal.dataloader

case class LoaderQueueItem[K, V](key: K, resolve: (V) => Unit, reject: (Exception) => Unit)
