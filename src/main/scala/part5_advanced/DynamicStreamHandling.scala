package part5_advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitches}

import scala.concurrent.duration._

object DynamicStreamHandling extends App {

  implicit val actorSystem = ActorSystem("DynamicStreamHandling")
  implicit val materializer = ActorMaterializer()
  import actorSystem.dispatcher

  // #1: Kill Switch

  val killSwitchFlow = KillSwitches.single[Int]

  val counter = Source(Stream.from(1)).throttle(1,1 second)
  val sink = Sink.ignore

  val killSwitch = counter
    .viaMat(killSwitchFlow)(Keep.right)
    .to(sink)
    .run()

  actorSystem.scheduler.scheduleOnce(3 seconds) {
    killSwitch.shutdown()
  }

  // Shared Kill Switch
  val anotherCounter = Source(Stream.from(1)).throttle(2, 1 second)
  val sharedKillSwitch = KillSwitches.shared("OneButtonToRuleThemAll")

  counter.via(sharedKillSwitch.flow).runWith(Sink.ignore)
  anotherCounter.via(sharedKillSwitch.flow).runWith(Sink.ignore)

  actorSystem.scheduler.scheduleOnce(3 seconds) {
    sharedKillSwitch.shutdown()
  }

  // MergeHub - Sharing the same sink to multiple streams at run time.

  val dynamicMerge = MergeHub.source[Int]
  val materializedSink = dynamicMerge.to(Sink.foreach[Int](println)).run()

  // Use the sink any time we like
  Source(1 to 10).runWith(materializedSink)
  counter.runWith(materializedSink)


  // BroadcastHuB - Sharing the same source to multiple streams at run time.
  val dynamicBroadcast = BroadcastHub.sink[Int]
  val materializedSource = Source(1 to 100).runWith(dynamicBroadcast)


  /**
   * Challange - combine a mergeHub and a broadcastHub
   *
   *  A publisher-subscribe component
   * */
  val merge = MergeHub.source[String]
  val bcast = BroadcastHub.sink[String]
  val (publisherPort, subscriberPort) = merge.toMat(bcast)(Keep.both).run()


  subscriberPort.runWith(Sink.foreach(e=> println(s"I receive: $e")))
  subscriberPort.map(string => string.length).runWith(Sink.foreach(n => println(s"I got a number: $n")))

  Source(List("Akka", "is", "amazing")).runWith(publisherPort)
  Source(List("I", "love", "scala")).runWith(publisherPort)
  Source.single("Streeeeeeemsss").runWith(publisherPort)



}
