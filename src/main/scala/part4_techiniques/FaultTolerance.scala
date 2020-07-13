package part4_techiniques

import akka.actor.ActorSystem
import akka.stream.Supervision.{Resume, Stop}
import akka.stream.{ActorAttributes, ActorMaterializer}
import akka.stream.scaladsl.{RestartSource, Sink, Source}

import scala.concurrent.duration._
import scala.util.Random

object FaultTolerance extends App {

  implicit val actorSystem = ActorSystem("FaultTolerance")
  implicit val materializer = ActorMaterializer()

  // 1 - Logging
  val faultSource = Source(1 to 10).map(e => if (e==6) throw new RuntimeException else e)
  faultSource.log("trackingElements").to(Sink.ignore).run()

  // 2 - Gracefully terminating a stream
  faultSource.recover {
    case _ : RuntimeException => Int.MinValue
  }.log("GracefulSource")
   .to(Sink.ignore)
   .run()

  // 3 - Recover with another stream
  faultSource.recoverWithRetries(3, {
    case _: RuntimeException => Source(90 to 99)
  }).log("RecoverWithRetries")
    .to(Sink.ignore)
    .run()

  // 4 - Backoff supervision
  val restartSource = RestartSource.onFailuresWithBackoff(
    minBackoff = 1 second,
    maxBackoff = 30 seconds,
    randomFactor = 0.2,
  )(() => {
    val randomNumber = new Random().nextInt(20)
    Source(1 to 10).map(elem => if (elem == randomNumber) throw new RuntimeException else elem)
  })

  restartSource.log("RestartBackoff").to(Sink.ignore).run()

  // 5 - Supervision Strategy
  val numbers = Source(1 to 20).map(n => if(n==13) throw new RuntimeException("Bad luck") else n).log("Supervision")
  val supervisedNumbers = numbers.withAttributes(ActorAttributes.supervisionStrategy({
    /*
      Resume = skip the faulty element
      Stop = stop the stream
      Restart = resume + clears internal state
    */
    case _: RuntimeException => Resume
    case _ => Stop
  }))

  supervisedNumbers.to(Sink.ignore).run()
}
