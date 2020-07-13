package part5_advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Attributes, Inlet, Outlet, SinkShape, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.mutable
import scala.util.Random

object CustomOperators extends App {

  implicit val system = ActorSystem("CustomOperators")
  implicit val materializer = ActorMaterializer()

  // 1 - A custom source which emits random numbers until canceled

  // Step 0 - define the shape
  class RandomNumberGenerator(max: Int) extends GraphStage[SourceShape[Int]] {

    // Step 1 - Define the ports and the components-specific members
    val random = new Random()
    val outPort = Outlet[Int]("randomGenerator")

    // Step 2 - Construct a new Shape
    override def shape: SourceShape[Int] = SourceShape(outPort)

    // Step 3 - Create the logic
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // Step 4:
      // Define Mutable state
      // implement my logic here
      setHandler(outPort, new OutHandler {
        // When there is demand from downstream
        override def onPull(): Unit = {
          // emit a new element
          val nextNumber = random.nextInt(max)
          // push it out of the outPort
          push(outPort, nextNumber)
        }
      })
    }
  }

  val randomGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100))
  randomGeneratorSource.runWith(Sink.foreach(println))

  // 2 - A custom sink that prints elements in batches of a given size

  class Bathcer(batchSize: Int) extends GraphStage[SinkShape[Int]] {

    val inPort = Inlet[Int]("batcher")

    override def shape: SinkShape[Int] = SinkShape(inPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // mutable state here
      val batch = new mutable.Queue[Int]

      override def preStart(): Unit = {
        pull(inPort)
      }

      setHandler(inPort, new InHandler {
        // When the upstream wants to send me an element
        override def onPush(): Unit = {
          val nextElement = grab(inPort)
          batch.enqueue(nextElement)

          // Assume some complex computation here
          Thread.sleep(1000)

          if(batch.size >= batchSize) {
            print("New batch: " + batch.dequeueAll(_ => true).mkString("[", ", ", "]"))
          }
          pull(inPort) // send demand upstream
        }

        override def onUpstreamFinish(): Unit = {
          if(batch.nonEmpty) {
            print("New batch: " + batch.dequeueAll(_ => true).mkString("[", ", ", "]"))
            println("Stream finished.")
          }
        }
      })
    }
  }

  val batcherSink = Sink.fromGraph(new Bathcer(10))

  randomGeneratorSource.to(batcherSink).run()

  /*
   * InHandler important methods
   * - onPush
   * - onUpstreamFinish
   * - OnUpstreamFailure
   *
   * ** Can check and retrieve the elements
   * pull: Signal demand
   * grab: take an element --> Fail if there is no element to grab
   * cancel: tell upstream to stop
   * isAvailable -> When the inputs port is available
   * hasBeenPulled -> Whether the element has been pulled
   * isClosed
  */

  /*
   * OutHandle important methods
   * - onPull
   * - onDownstreamFinish
   * ** No onDownstreamFailure as I'll receive a cancel signal
   *
   * ** Can check and retrieve the elements
   * push: send an element
   * complete: Finish the stream
   * fail -> Terminate the stream with an exception
   * isAvailable -> When the inputs port is available
   * isClosed
  */

}
