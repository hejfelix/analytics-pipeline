import mill._
import mill.define.Target
import mill.util.Loose
import scalalib._

object V {
    val akka          = "2.5.18"
    val akkaHttp      = "10.1.5"
    val akkaHttpCirce = "1.22.0"
    val alpakkaKafka  = "1.0-M1"
    val cats          = "1.5.0"
    val circe         = "0.10.0"
    val frameless     = "0.7.0"
    val kafkaClients  = "2.0.0"
    val newtype       = "0.4.2"
    val spark         = "2.3.1"
  }

val scala212 = "2.12.7"
val scala211 = "2.11.12"

val scalac211Flags =
    Seq(
      "-deprecation", // Emit warning and location for usages of deprecated APIs.
      "-encoding",
      "utf-8", // Specify character encoding used by source files.
      "-explaintypes", // Explain type errors in more detail.
      "-feature", // Emit warning and location for usages of features that should be imported explicitly.
      "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
      "-language:experimental.macros", // Allow macro definition (besides implementation and application)
      "-language:higherKinds", // Allow higher-kinded types
      "-language:implicitConversions", // Allow definition of implicit functions called views
      "-unchecked", // Enable additional warnings where generated code depends on assumptions.
      "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
      "-Xfatal-warnings", // Fail the compilation if there are any warnings.
      "-Xfuture", // Turn on future language features.
      "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
      "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
      "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
      "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
      "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
      "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
      "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
      "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
      "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
      "-Xlint:option-implicit", // Option.apply used implicit view.
      "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
      "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
      "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
      "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
      "-Xlint:unsound-match", // Pattern match may not be typesafe.
      "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
      "-Ypartial-unification", // Enable partial unification in type constructor inference
      "-Ywarn-dead-code", // Warn when dead code is identified.
      "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
      "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
      "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
      "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
      "-Ywarn-numeric-widen", // Warn when numerics are widened.
      "-Ywarn-value-discard", // Warn when non-Unit expression results are unused.
      "-Ypartial-unification"
    )

val scalac212Flags = Seq(
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals", // Warn if a local definition is unused.
    "-Ywarn-unused:params", // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates", // Warn if a private member is unused.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Xlint:constant" // Evaluation of a constant arithmetic expression results in an error.
  ) ++ scalac211Flags

object ingestion extends ScalaModule {
    override def scalacOptions: Target[Seq[String]] = scalac212Flags
    override def scalaVersion: Target[String]       = scala212
    override def moduleDeps: Seq[JavaModule]        = Seq(models(scala212))
    override def ivyDeps: Target[Loose.Agg[Dep]] = Agg(
      ivy"com.typesafe.akka::akka-http:${V.akkaHttp}",
      ivy"com.typesafe.akka::akka-stream:${V.akka}",
      ivy"com.typesafe.akka::akka-stream-kafka:${V.alpakkaKafka}",
      ivy"de.heikoseeberger::akka-http-circe:${V.akkaHttpCirce}",
      ivy"io.circe::circe-core:${V.circe}",
      ivy"io.circe::circe-generic:${V.circe}",
      ivy"io.circe::circe-parser:${V.circe}",
      ivy"io.circe::circe-java8:${V.circe}",
      ivy"io.estatico::newtype:${V.newtype}"
    )
  }

object models extends mill.Cross[Models](scala211, scala212)

class Models(override val crossScalaVersion: String) extends CrossScalaModule {
    override def scalacPluginIvyDeps: Target[Loose.Agg[Dep]] =
      Agg(ivy"org.scalamacros:paradise_$crossScalaVersion:2.1.1")
    override def ivyDeps: Target[Loose.Agg[Dep]] = Agg(
      ivy"io.circe::circe-core:${V.circe}",
      ivy"io.circe::circe-generic:${V.circe}",
      ivy"io.circe::circe-java8:${V.circe}",
      ivy"io.estatico::newtype:${V.newtype}",
      ivy"org.apache.kafka:kafka-clients:${V.kafkaClients}",
    )
    override def scalacOptions: Target[Seq[String]] =
      crossScalaVersion match {
        case `scala211` => scalac211Flags
        case `scala212` => scalac212Flags
      }
  }

object analytics extends ScalaModule {

    val version                                              = scala211
    def scalaVersion: Target[String]                         = version
    override def forkArgs: Target[Seq[String]]               = Seq("-Dlog4j.configuration=log4j.properties")
    override def scalacPluginIvyDeps: Target[Loose.Agg[Dep]] = Agg(ivy"org.scalamacros:paradise_$version:2.1.1")
    override def moduleDeps: Seq[JavaModule]                 = Seq(models(scala211))
    override def scalacOptions: Target[Seq[String]]          = scalac211Flags

    override def ivyDeps: Target[Loose.Agg[Dep]] = Agg(
      ivy"org.apache.spark::spark-core:${V.spark}",
      ivy"org.apache.spark::spark-streaming:${V.spark}",
      ivy"org.typelevel::frameless-dataset:${V.frameless}",
      ivy"org.typelevel::frameless-cats:${V.frameless}",
      ivy"io.circe::circe-core:${V.circe}",
      ivy"io.circe::circe-generic:${V.circe}",
      ivy"io.circe::circe-parser:${V.circe}",
      ivy"io.circe::circe-java8:${V.circe}",
      ivy"org.typelevel::cats-core:${V.cats}",
      ivy"org.apache.spark::spark-streaming-kafka-0-10:${V.spark}"
    )

  }
