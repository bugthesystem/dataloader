# DataLoader

> DataLoader is a generic utility to be used as part of your application's data fetching layer to provide a consistent API over various backends and reduce requests to those backends via batching and caching.

### Usage

```scala
      val identityLoader = new DataLoader[Int, Int](
        batchLoadFn = (keys: Array[Int]) => Some(Promise().success(keys))
      )

      val f: Future[Int] = identityLoader.load(1)

      val value: Int = Await.result(f, 1.seconds)

      println(s"Resolved value: $value")
```


### Credits
> TODO:
@z i λ a s a l
