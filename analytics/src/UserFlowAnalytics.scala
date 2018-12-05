import java.time.Instant
import java.util.UUID

import models.{ActionEvent, UserId}
import org.apache.spark.streaming.dstream.DStream

case class UserFlowAnalytics() {

  val setup: DStream[(UserId, ActionEvent)] => Unit = actionEvents => {

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
          println(s"Initial state: ${initialState(stateMachine)}")
          println(s"Most probable flow: ${mostLikelyFlow(stateMachine)(steps = 10)}")
          println(s"Most uncertain junction: ${mostUncertainJunction(stateMachine)}")
      }

      if (firstNum.length > num) println("...")
      println()

    }
  }

  private def mostUncertainJunction(states: Map[String, Map[String, Double]]): Option[(String, Map[String, Double])] =
    if (states.isEmpty) {
      None
    } else {
      Option(states.maxBy {
        case (_, transitions) =>
          val biggestNumberOfEquallyValidChoices: (Double, Int) =
            transitions.toList.groupBy { case (_, frequency) => frequency }.mapValues(_.length).maxBy(_._2)
          biggestNumberOfEquallyValidChoices._2
      })
    }

  private def initialState(states: Map[String, Map[String, Double]]): Option[String] = {
    val incomingArrows: Set[String]                  = states.values.flatMap(_.keys).toSet
    val statesWithNoIncomingArrows: Iterable[String] = states.keys.filterNot(incomingArrows.contains)
    statesWithNoIncomingArrows.headOption
  }

  /**
    * Given a probabilistic state machine, a list of most probable state transitions is given
    *
    * @param states The probabilistic state map
    * @param steps  The number of steps to produce
    * @return The most likely flow for n steps, ties broken arbitrarily but consistently
    */
  def mostLikelyFlow(states: Map[String, Map[String, Double]])(steps: Int): List[String] = {
    val result = Stream
      .iterate(List.empty[String], steps) {
        case Nil if states.nonEmpty => initialState(states).toList
        case currentResult @ x :: _ if states.contains(x) =>
          val mostLikelyNextState = states(x).maxBy { case (_, frequency) => frequency }._1
          mostLikelyNextState :: currentResult
        case x => x
      }
      .takeWhile(xs => xs.distinct.sorted == xs.sorted) //no cycles
      .toList
    result.lastOption.getOrElse(Nil).reverse
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
    */
  lazy val analyzeFlows: List[ActionEvent] => Map[ActionEvent, Map[ActionEvent, Double]] = events => {

    val chronological: List[ActionEvent] =
      events.sortBy(_.timeStamp.getOrElse(Instant.MIN))

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
      case (origin: ActionEvent, _) => origin.copy(timeStamp = None)
    }.mapValues(_.collect { case (_, target) => target })

  /**
    * Create a frequency map from a list of elements
    *
    * @return A key->frequency map derived from xs
    */
  lazy val frequencyMap: List[ActionEvent] => Map[ActionEvent, Double] = xs =>
    xs.map(_.copy(timeStamp = None))
      .groupBy(identity)
      .mapValues(_.size.toDouble / xs.size)

}
