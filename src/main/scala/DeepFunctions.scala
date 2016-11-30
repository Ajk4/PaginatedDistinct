import org.apache.spark.rdd.RDD

import scala.collection._

object DeepFunctions {

  def paginatedDistinct[T](rdd: RDD[T], pageSize: Int)(implicit ord: Ordering[T]): Set[T] = {
    PaginatedDistinct[T](pageSize).calculate(rdd)
  }

  def paginatedDistinct[T](rdd: RDD[T], pageSize: Int, lastValue: T)(implicit ord: Ordering[T]): Set[T] = {
    PaginatedDistinct[T](pageSize).calculate(rdd.filter(x => ord.gt(x, lastValue)))
  }

  private case class PaginatedDistinct[T](pageSize: Int)(implicit ord: Ordering[T]) {

    type Aggregator = UniquePriorityQueue[T]

    def calculate(rdd: RDD[T]): Set[T] = rdd.aggregate(zero)(mergeValue, mergeCombiners).allElements

    private val zero = new UniquePriorityQueue[T]()

    private def mergeValue(acc: Aggregator, next: T): Aggregator = {
      acc.enqueue(next)
      truncate(acc)
      acc
    }

    private def mergeCombiners(left: Aggregator, right: Aggregator): Aggregator = {
      for (elem <- right.allElements) {
        left.enqueue(elem)
      }
      truncate(left)
      left
    }

    private def truncate(aggregator: Aggregator): Unit =
      while (aggregator.size > pageSize) {
        aggregator.dequeue()
      }
  }

  class UniquePriorityQueue[T] (implicit val ordering: Ordering[T]) extends Serializable {

    private val priorityQueue = mutable.PriorityQueue.empty[T]
    val allElements = mutable.Set.empty[T]

    def size = allElements.size

    def enqueue(elem: T): Unit = {
      if(!allElements.contains(elem)){
        allElements += elem
        priorityQueue.enqueue(elem)
      }
    }

    def dequeue(): T = {
      val elem = priorityQueue.dequeue()
      allElements -= elem
      elem
    }

  }

}
