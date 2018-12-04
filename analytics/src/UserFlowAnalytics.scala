import java.time.Instant
import java.util.UUID

import models.{ActionEvent, UserId}
import org.apache.spark.streaming.dstream.DStream

case class UserFlowAnalytics() {

  val setup: DStream[(UserId, ActionEvent)] => Unit = actionEvents => {
    //Key for timestamps
    val timestamp: String = "timestamp"
    //Key for event IDs
    val eventId: String = "eventId"

    /**
      * Spark does runtime reflection for serialization, so we can't use
      * newtypes as keys here.
      */
    val groupedByUser: DStream[(UUID, List[ActionEvent])] =
      actionEvents
        .map {
          case (id, event) => id.id -> List(event)
        }
        .reduceByKey(_ ++ _)

    val states: DStream[(UUID, Map[ActionEvent, Map[ActionEvent, Double]])] =
      groupedByUser.mapValues(analyzeFlows)

//    states.foreachRDD(_.foreach(visualize))
//    states.mapValues(pretty).print
    val dumbMaps: DStream[(UUID, Map[String, Map[String, Double]])] = states.mapValues(stateMachine =>
      stateMachine.map {
        case (key, value) =>
          key.eventName -> value.map {
            case (key, value) => key.eventName -> value
          }
    })

    dumbMaps.foreachRDD { (rdd, time) =>
      val num      = 10
      val firstNum = rdd.take(num + 1)
      println("-------------------------------------------")
      println(s"Time: ${Instant.ofEpochMilli(time.milliseconds)}")
      println("-------------------------------------------")
      firstNum.take(num).foreach {
        case (id, stateMachine) =>
          println(s"$id:\n${pretty(stateMachine)}")
          println()
          println(s"Most probable flow: ${mostLikelyFlow(stateMachine)(steps = 10)}")
      }

      if (firstNum.length > num) println("...")
      println()

    }
//
//  .foreachRDD { (rdd: RDD[(UUID, Map[ActionEvent, Map[ActionEvent, Double]])], time: Time) =>
//      {
//        val num = 10
//        val firstNum = rdd.take(num + 1).map {
//          case (id, stateMachine) => (id, (stateMachine, pretty(stateMachine)))
//        }
//        println("-------------------------------------------")
//        println(s"Time: ${Instant.ofEpochMilli(time.milliseconds)}")
//        println("-------------------------------------------")
//        firstNum.take(num).foreach {
//          case (id, (stateMachine, prettyStateMachine)) =>
//            println(s"$id:\n$prettyStateMachine")
//            println()
////            println(s"Most probable flow: ${mostLikelyFlow(None)(stateMachine.map {
////              case (state, propMap) => state.eventName -> propMap
////            })(steps = 10)}")
//        }
//
//        if (firstNum.length > num) println("...")
//        println()
//      }

//  states.map(map => mostLikelyFlow(None)(map)(steps = 10)).print
  }

  /**
    * Given a probabilistic state machine, a list of most probable state transitions is given
    *
    * @param startState "" for entry state, otherwise the seed state of the computation
    * @param states The probabilistic state map
    * @param steps  The number of steps to produce
    * @return The most likely flow for n steps, ties broken arbitrarily but consistently
    */
  def mostLikelyFlow(states: Map[String, Map[String, Double]])(steps: Int): List[String] = {
    val result = Stream
      .iterate(List.empty[String], steps) {
        case Nil if states.nonEmpty => List(states.head._1)
        case currentResult @ x :: _ if states.contains(x) =>
          val mostLikelyNextState = states(x).maxBy { case (_, frequency) => frequency }._1
          mostLikelyNextState :: currentResult
        case x => x
      }
      .takeWhile(xs => xs.distinct.sorted == xs.sorted) //no cycles
      .toList
    result.lastOption.getOrElse(Nil)
  }
  private lazy val pretty: Map[String, Map[String, Double]] => String =
    _.map {
      case (from, tos) =>
        from -> tos.map {
          case (choice, probability) => s"$choice~$probability"
        }
    }.mkString("\n")

  /**
    * Analyzes an event stream as user flows (distribution of user flows in application)
    *
    * @param events Key-Value descriptions of each event. All events contain 'timestamp' keys.
    * @return A DStream with transitions by state
    */
  lazy val analyzeFlows: List[ActionEvent] => Map[ActionEvent, Map[ActionEvent, Double]] = events => {

    val chronological: List[ActionEvent] =
      events.sortBy(_.timeStamp.getOrElse(Instant.MIN))(Ordering[Instant].reverse)

    val transitions: List[(ActionEvent, ActionEvent)] = chronological.zip(chronological.drop(1))

    val transitionByState: Map[ActionEvent, Map[ActionEvent, Double]] =
      groupTransitionsByEventID(transitions).mapValues(frequencyMap)

    transitionByState
  }

  /**
    * Groups transitions by their event IDs
    *
    * @param transitions A list of transition pairs as (String,String) tuples
    * @return A map of possible transitions from each possible state
    */
  lazy val groupTransitionsByEventID: List[(ActionEvent, ActionEvent)] => Map[ActionEvent, List[ActionEvent]] =
    _.groupBy {
      case (origin: ActionEvent, _) => origin: ActionEvent
    }.mapValues(_.collect { case (_, target) => target })
//    transitions.groupBy(_._1).mapValues(_.map(_._2))

  /**
    * Create a frequency map from a list of elements
    *
    * @param xs The list for which to create the frequency map
    * @tparam T The type of elements in the list
    * @return A key->frequency map derived from xs
    */
  lazy val frequencyMap: List[ActionEvent] => Map[ActionEvent, Double] = xs =>
    xs.map(_.copy(timeStamp = None))
      .groupBy(identity)
      .mapValues(_.size.toDouble / xs.size)

}
