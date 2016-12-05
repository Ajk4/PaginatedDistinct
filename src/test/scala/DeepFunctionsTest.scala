import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

class DeepFunctionsTest extends FunSuite with Matchers {

  test("testPaginatedDistinct") {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq(
      "A", "A", "A", "B", "C", "D", "A", "B",
      "A", "A", "A", "B", "C", "D", "A", "B",
      "A", "A", "A", "B", "C", "D", "A", "B",
      "A", "A", "A", "B", "C", "D", "A", "B",
      "E", "F", "A", "G"
    ), 4)

    DeepFunctions.paginatedDistinct(rdd, 2) shouldEqual Set("A", "B")
    DeepFunctions.paginatedDistinct(rdd, 2, "B") shouldEqual Set("C", "D")
    DeepFunctions.paginatedDistinct(rdd, 2, "D") shouldEqual Set("E", "F")
    DeepFunctions.paginatedDistinct(rdd, 2, "F") shouldEqual Set("G")

  }

}
