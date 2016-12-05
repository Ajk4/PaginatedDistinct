import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

class DeepFunctionsPerformanceTest extends FunSuite with Matchers {

  test("testPaginatedDistinct performance") {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val distinctData = Seq("A", "D", "C", "E", "F", "AA", "BB", "CC", "DDAD", "GASD",
    "a", "b", "c", "abcd")
    val data = for (i <- 1 to 1000000) yield distinctData(i % distinctData.size)

    val rdd = sc.parallelize(data, 100)

    timed("Paginated distinct") {
      DeepFunctions.paginatedDistinct(rdd, 20)
    }

    timed("Normal distinct") {
      rdd.distinct.collect
    }
  }

  def timed[T](name: String)(code: => T): T = {
    val timeStart = System.currentTimeMillis()
    val result = code
    val timeEnd = System.currentTimeMillis()
    info(s"$name in millis: ${timeEnd - timeStart}")
    result
  }

}
