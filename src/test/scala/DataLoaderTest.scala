import net.github.ziyasal.dataloader.DataLoader
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.duration.DurationInt

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}

class DataLoaderTest extends FunSpec with Matchers {
  describe("Primary API") {

    it("builds a really really simple data loader") {
      val identityLoader = new DataLoader[Int, Int](
        batchLoadFn = (keys: Array[Int]) => Some(Promise().success(keys))
      )

      val f: Future[Int] = identityLoader.load(1)

      f.isInstanceOf[Future[Int]] shouldBe true

      val value: Int = Await.result(f, 5.seconds)

      println(s"Resolved value: $value")

      value shouldBe 1
    }
  }
}
