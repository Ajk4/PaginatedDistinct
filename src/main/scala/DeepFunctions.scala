import org.apache.spark.rdd.RDD

import scala.collection._

object DeepFunctions {

  def paginatedDistinct[T](rdd: RDD[T], pageSize: Int)(implicit ord: Ordering[T]): Set[T] = {
    PaginatedDistinct[T](pageSize).calculate(rdd)
  }

  def paginatedDistinct[T](rdd: RDD[T], pageSize: Int, lastValue: T)(implicit ord: Ordering[T]): Set[T] = {
    PaginatedDistinct[T](pageSize).calculate(rdd.filter(x => ord.gt(x, lastValue)))
  }

  case class PaginatedDistinct[T](pageSize: Int)(implicit ord: Ordering[T]) {

    type Aggregator = mutable.SortedSet[T]

    def calculate(rdd: RDD[T]): Set[T] = rdd.aggregate(zero)(mergeValue, mergeCombiners)

    private val zero = mutable.SortedSet.empty[T]

    private def mergeValue(acc: Aggregator, next: T): Aggregator = {
      acc += next
      truncate(acc)
      acc
    }

    private def mergeCombiners(leftSet: Aggregator, rightSet: Aggregator): Aggregator = {
      leftSet ++= rightSet
      truncate(leftSet)
      leftSet
    }

    // TODO Better implementation of removing elements from mutable collection
    private def truncate(aggregator: Aggregator): Unit =
      while (aggregator.size > pageSize) {
        // last iterates through full collection
        aggregator -= aggregator.last
      }
  }

}
