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

    def calculate(rdd: RDD[T]): Set[T] = rdd.aggregate(zero)(mergeValue, mergeCombiners).elementsInQueue

    private val zero = new UniquePriorityQueue[T]()

    private def mergeValue(acc: Aggregator, next: T): Aggregator = {
      acc.enqueue(next)
      truncate(acc)
      acc
    }

    private def mergeCombiners(left: Aggregator, right: Aggregator): Aggregator = {
      for (elem <- right.elementsInQueue) {
        left.enqueue(elem)
      }
      truncate(left)
      left
    }

    private def truncate(aggregator: Aggregator): Unit =
      while (aggregator.elementsInQueue.size > pageSize) {
        aggregator.dequeue()
      }
  }

  class UniquePriorityQueue[T](implicit val ordering: Ordering[T]) extends Serializable {

    import java.util.{PriorityQueue => JPriorityQueue}

    // Scala's queue works like 'smallest value first'
    // Java's queue works like 'largest value first'
    // Ordering is reversed to keep scala interface
    private val priorityQueue = new JPriorityQueue[T](5, ordering.reverse)
    private val elementsMutableSet = mutable.Set.empty[T]

    def elementsInQueue: Set[T] = elementsMutableSet

    def enqueue(elem: T): Unit = {
      if (!elementsMutableSet.contains(elem)) {
        elementsMutableSet += elem
        priorityQueue.offer(elem)
      }
    }

    def dequeue(): T = {
      val elem = priorityQueue.poll()
      elementsMutableSet -= elem
      elem
    }

  }

}
