import java.util.UUID

import io.circe.Decoder
import io.circe.Decoder.Result
package object models {
  import io.estatico.newtype.macros.newtype

  object UserId {
    implicit val dec = new Decoder[UserId] {
      override def apply(c: _root_.io.circe.HCursor): Result[UserId] =
        c.as[UUID].right.map(id => UserId(id))
    }
  }
  @newtype case class UserId(id: UUID)
}
