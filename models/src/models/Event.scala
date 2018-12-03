package models

import java.time.Instant

sealed trait Event {
  val userId: UserId
  val timeStamp: Option[Instant]
}

final case class ActionEvent(
    userId: UserId, // Unique id for the user emitting this event
    eventName: String, // Name for all instances of this event (e.g. go to page A, click button B)
    timeStamp: Option[Instant],
    description: String
) extends Event
