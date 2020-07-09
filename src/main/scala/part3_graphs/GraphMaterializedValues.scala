package part3_graphs

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, SinkShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object GraphMaterializedValues extends App {

  implicit val system = ActorSystem("GrapMaterializedValues")
  implicit val materializer = ActorMaterializer()

  val wordSource = Source(List("Akka", "is", "awesome",  "rock", "the", "jvm"))
  val printer = Sink.foreach[String](println)
  val counter = Sink.fold[Int, String](0)((count, _) => count + 1)

  /*
  A composite component (sink)
  - prints out all strings which are lowercase
  - Counts the string that are short (< 5 chars)
  */

  val complexWordSink = Sink.fromGraph(
    GraphDSL.create(printer, counter)((printerMatValue, counterMatValue) => counterMatValue) {
      implicit builder => (printerShape, counterShape) =>

      import GraphDSL.Implicits._

      // step 2  - Shapes
      val broadcast = builder.add(Broadcast[String](2))
      val lowercaseFilter = builder.add(Flow[String].filter(word => word == word.toLowerCase))
      val shortStringFilter = builder.add(Flow[String].filter(_.length < 5))

      // step 3 - connections
      broadcast ~> lowercaseFilter ~> printerShape
      broadcast ~> shortStringFilter ~> counterShape

      // Step 4 - the Shape
      SinkShape(broadcast.in)
    }
  )


  val shortStringCountFuture = wordSource.toMat(complexWordSink)(Keep.right).run()

  import system.dispatcher
  shortStringCountFuture.onComplete {
    case Success(count) => println(s"The total number of short string is: $count")
    case Failure(exception) => println(s"The count of short string failed: $exception")
  }

  /**
   * Exercise Implements the functions below that transform the materialized value from the flow
   * and coounts the number os elements that pass through the flow
   */
  /*
    Hint: use a broadcast and a Sink.fold
   */
  def enhanceFlow[A, B](flow: Flow[A, B, _]): Flow[A, B, Future[Int]] = {
    val counterSink = Sink.fold[Int, B](0)((count, _) => count + 1)
    Flow.fromGraph(
      GraphDSL.create(counterSink) {
        implicit builder => counterSinkShape =>
        import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[B](2))
          val originalFlowShape = builder.add(flow)

          // step 3
          originalFlowShape ~> broadcast ~> counterSinkShape

          // step 4 - Shape
          FlowShape(originalFlowShape.in , broadcast.out(1))
      }
    )
  }

  val simpleFlow = Flow[Int].map(x => x)
  val simpleSource = Source(1 to 42)
  val simpleSink = Sink.ignore

  val enhancedFlowCountFuture = simpleSource.viaMat(enhanceFlow(simpleFlow))(Keep.right).toMat(simpleSink)(Keep.left).run()


  enhancedFlowCountFuture.onComplete {
    case Success(count) => println(s"$count elements wen though the enhanced flow")
    case _ => println("Something failed")
  }



}
