import org.apache.spark.rdd.RDD

import scala.collection._
import java.util.TreeSet

object DeepFunctions {

  def paginatedDistinct[T](rdd: RDD[T], pageSize: Int)(implicit ord: Ordering[T]): Set[T] = {
    PaginatedDistinct[T](pageSize).calculate(rdd)
  }

  def paginatedDistinct[T](rdd: RDD[T], pageSize: Int, lastValue: T)(implicit ord: Ordering[T]): Set[T] = {
    PaginatedDistinct[T](pageSize).calculate(rdd.filter(x => ord.gt(x, lastValue)))
  }

  private case class PaginatedDistinct[T](pageSize: Int)(implicit ord: Ordering[T]) {

    import collection.JavaConverters._

    type Aggregator = TreeSet[T]

    def calculate(rdd: RDD[T]): Set[T] = rdd.aggregate(zero)(mergeValue, mergeCombiners).asScala

    private val zero = new TreeSet[T](ord)

    private def mergeValue(acc: Aggregator, next: T): Aggregator = {
      acc.add(next)
      truncate(acc)
      acc
    }

    private def mergeCombiners(left: Aggregator, right: Aggregator): Aggregator = {
      left.addAll(right)
      truncate(left)
      left
    }

    private def truncate(aggregator: Aggregator): Unit = {
      while (aggregator.size > pageSize) {
        aggregator.pollLast()
      }
    }

  }

}
