package part4_techiniques

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

import scala.concurrent.Future

object IntegratingWithExternalServices extends App {
  implicit val system: ActorSystem = ActorSystem("IntegratingWithExternalServices")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  // import system.dispatcher //not recommended in pratice for mapAsync

  implicit val dispatcher = system.dispatchers.lookup("dedicated-dispatcher")

  def genericExternalService[A, B](element: A): Future[B] = ???

  // example: Simplified PagerDuty
  case class PagerEvent(application: String, description: String, date: Date)

  val eventSource = Source(List(
    PagerEvent("AkkaInfra", "Infrastructure broke", new Date),
    PagerEvent("FastDataPipeline", "Illegal element in the data pipeline", new Date),
    PagerEvent("AkkaInfra", "A service stopped responding", new Date),
    PagerEvent("SuperFrontend", "A button doesn't work", new Date),
  ))


  object PageService {
    private val enginers = List("Daniel","John", "Ladu Gaga")
    private val emails = Map (
      "Daniel" -> "daniel@rockthejvm.com",
      "John" -> "john@rockthejvm.com",
      "Lady Gaga" -> "ladygaga@rtjv.com"
    )

    def processEvent(pagerEvent: PagerEvent): Future[String] = Future {
      val enginerIndex = (pagerEvent.date.toInstant.getEpochSecond / (24*3600)) % enginers.length
      val enginer = enginers(enginerIndex.toInt)
      val enginerEmail = emails(enginer)

      // Page the engineer
      println(s"Sending engineer $enginerEmail a high priority notification: $pagerEvent")
      Thread.sleep(1000)

      // return the email that was paged
      enginerEmail
    }
  }

  val infraEvents = eventSource.filter(_.application == "AkkaInfra")
  val pagedEngineerEmail = infraEvents.mapAsync(parallelism = 4)(event => PageService.processEvent(event))
  // guaranteed the relative order of elements
  // mapAsyncUnordered => does not guaranteed the order

  val pagedEmailsSink = Sink.foreach[String](email => println(s"Successfully sent notificaiton to $email"))
  pagedEngineerEmail.to(pagedEmailsSink).run()


  class PageActor extends Actor with ActorLogging {
    private val enginers = List("Daniel","John", "Ladu Gaga")
    private val emails = Map (
      "Daniel" -> "daniel@rockthejvm.com",
      "John" -> "john@rockthejvm.com",
      "Lady Gaga" -> "ladygaga@rtjv.com"
    )

    private def processEvent(pagerEvent: PagerEvent): Future[String] = Future {
      val enginerIndex = (pagerEvent.date.toInstant.getEpochSecond / (24*3600)) % enginers.length
      val enginer = enginers(enginerIndex.toInt)
      val enginerEmail = emails(enginer)

      // Page the engineer
      println(s"Sending engineer $enginerEmail a high priority notification: $pagerEvent")
      Thread.sleep(1000)

      // return the email that was paged
      enginerEmail
    }

    override def receive: Receive = {
      case pagerEvent: PagerEvent =>
        sender() ! processEvent(pagerEvent)
    }
  }

  import akka.pattern.ask
  import scala.concurrent.duration._
  implicit val timeout = Timeout(3 seconds)
  val pagerActor = system.actorOf(Props[PageActor], "pagerActor")
  val alternativePagedEngineerEmails = infraEvents.mapAsync(parallelism = 4)(event => (pagerActor ? event).mapTo[String])
  alternativePagedEngineerEmails.to(pagedEmailsSink).run()

  // do not confuse mapAsync with async (ASYNC boundary)

}
