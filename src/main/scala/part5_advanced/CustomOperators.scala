package part5_advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Outlet, SinkShape, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Random, Success, Try}

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


  /**
   * Exercise: A custom flow - a simple filter flow
   * - 2 ports: an input port and an output port
   */

  class FilterFlow[T](predicate: T => Boolean) extends GraphStage[FlowShape[T, T]] {

    val inPort = Inlet[T]("filterFlowIn")
    val outPort = Outlet[T]("filterFlowOut")

    override def shape: FlowShape[T, T] = FlowShape(inPort, outPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      override def preStart(): Unit = {
        pull(inPort)
      }

      setHandler(outPort, new OutHandler {
        override def onPull(): Unit = pull(inPort)
      })

      setHandler(inPort, new InHandler {
        override def onPush(): Unit = {
         Try {
           val element = grab(inPort)
           if(predicate(element)) push(outPort, element) // pass it on
           else pull(inPort) // ask for another element
         } .recover { case ex: Throwable => failStage(ex) }
        }
      })
    }
  }

  val myFilter = Flow.fromGraph(new FilterFlow[Int](_ > 50))

  randomGeneratorSource.via(myFilter).to(batcherSink).run()
  // backpressure ou of the box !!!!


  /**
   * Materialized values in graph stages
   * */

  // 3 - A flow that counts the number of elements that go through it
  class CounterFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Int]] {

    val inPort = Inlet[T]("counterIn")
    val outPort = Outlet[T]("counterOut")

    override val shape = FlowShape(inPort, outPort)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Int]) = {
      val promise = Promise[Int]

      val logic = new GraphStageLogic(shape) {
        // setting mutable state
        var counter = 0

        setHandler(outPort, new OutHandler {
          override def onPull(): Unit = pull(inPort)

          override def onDownstreamFinish(): Unit = {
            promise.success(counter)
            super.onDownstreamFinish()
          }
        })

        setHandler(inPort, new InHandler {
          override def onPush(): Unit = {
            // extract the element
            val nextElement = grab(inPort)
            counter += 1
            // pass it on
            push(outPort, nextElement)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(counter)
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }
        })
      }

      (logic, promise.future)
    }
  }

  val counterFlow = Flow.fromGraph(new CounterFlow[Int])
  val countFuture = Source(1 to 10)
//    .map( x => if(x==7) throw new RuntimeException("gotcha") else x)
    .viaMat(counterFlow)(Keep.right)
//    .to(Sink.foreach(x => if (x==7) throw new RuntimeException("gotcha sink!") else println(x)))
    .to(Sink.foreach[Int](println))
    .run()

  import system.dispatcher

  countFuture.onComplete {
    case Success(value) => println(s"The number of elements passed: $value")
    case Failure(exception) =>  println(s"Counting the elements failed: $exception")
  }
}
