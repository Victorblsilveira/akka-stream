import scala.language.postfixOps

import akka.actor.{Actor, ActorSystem, PoisonPill, Props, Stash}
import akka.util.Timeout

object AkkaRecap extends App {

  class SimpleActor extends Actor with Stash {
    override def receive: Receive = {
      case "createChield" =>
        val childActor = context.actorOf(Props[SimpleActor], "simpleActor")
        childActor ! "hello"

      case "stashThis" =>
        stash()

      case "change handler Now" =>
        unstashAll()
        context.become(anotherBehavior)

      case message => println(s"Look at this message: $message")
    }

    def anotherBehavior: Receive = {
      case message => println(s"In another receive behavior: $message")
    }
  }

  val system = ActorSystem("AkkaRecap")

  val actor = system.actorOf(Props[SimpleActor], "SimpleActor")

  actor ! "omg what a message"

  actor ! PoisonPill


  import scala.concurrent.duration._
  import system.dispatcher

  system.scheduler.scheduleOnce(2 seconds){
    actor ! "delayed happy birthday!"
  }

  import akka.pattern.ask
  implicit val timeout = Timeout(3 seconds)

  val future = actor ? "question"


  import akka.pattern.pipe
  val anotherActor = system.actorOf(Props[SimpleActor], "anotherSimpleActor")
  future.mapTo[String].pipeTo(anotherActor)
}
