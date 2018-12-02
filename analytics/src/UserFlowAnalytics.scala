import java.time.Instant
import java.util.UUID

import models.{ActionEvent, Event}
import org.apache.spark.streaming.dstream.DStream

case class UserFlowAnalytics() {

  val setup = (events: DStream[Event]) => {
    //Key for timestamps
    val timestamp: String = "timestamp"
    //Key for event IDs
    val eventId: String = "eventId"

    val actionEvents: DStream[ActionEvent] = events.transform(_.collect {
      case ae: ActionEvent => ae
    })

    val groupedByUser: DStream[(UUID, Iterable[ActionEvent])] =
      actionEvents
        .transform(_.groupBy(_.userId.id))

    val states: DStream[(UUID, Map[ActionEvent, Map[ActionEvent, Double]])] =
      groupedByUser.mapValues(analyzeFlows)

//    states.foreachRDD(_.foreach(visualize))
    states.mapValues(pretty).print
//  states.map(map => mostLikelyFlow(None)(map)(steps = 10)).print
  }

  private lazy val pretty: Map[ActionEvent, Map[ActionEvent, Double]] => String =
    _.map {
      case (from, tos) =>
        from.eventName -> tos.map {
          case (choice, probability) => s"${choice.eventName}~${probability}"
        }
    }.mkString("\n")

  /**
    * Analyzes an event stream as user flows (distribution of user flows in application)
    *
    * @param events Key-Value descriptions of each event. All events contain 'timestamp' keys.
    * @return A DStream with transitions by state
    */
  lazy val analyzeFlows: Iterable[ActionEvent] => Map[ActionEvent, Map[ActionEvent, Double]] = events => {

    val chronological: List[ActionEvent] =
      events.toList.sortBy(_.timeStamp.getOrElse(Instant.MIN))(Ordering[Instant].reverse)

    val transitions: List[(ActionEvent, ActionEvent)] = chronological.zip(chronological.drop(1))

    val transitionByState: Map[ActionEvent, Map[ActionEvent, Double]] =
      groupTransitionsByEventID(transitions).mapValues(frequencyMap)

    transitionByState
  }

  /**
    * Given a probabilistic state machine, a list of most probable state transitions is given
    *
    * @param startState "" for entry state, otherwise the seed state of the computation
    * @param states The probabilistic state map
    * @param steps  The number of steps to produce
    * @return The most likely flow for n steps, ties broken arbitrarily but consistently
    */
  def mostLikelyFlow(startState: Option[Event])(states: Map[Event, Map[Event, Double]])(steps: Int): List[Event] =
    startState match {
      case Some(start) if states.isEmpty || steps == 0 || !states.contains(start) => Nil
      case None if states.nonEmpty =>
        val start                           = states.keys.head
        val (mostLikelyNextState: Event, _) = states(start).toList.minBy { case (_, probability) => probability }
        start :: mostLikelyFlow(Option(mostLikelyNextState))(states)(steps - 1)
      case Some(start) =>
        val (mostLikelyNextState: Event, _) = states(start).toList.minBy { case (_, probability) => probability }
        start :: mostLikelyFlow(Option(mostLikelyNextState))(states)(steps - 1)
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
