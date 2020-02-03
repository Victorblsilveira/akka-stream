package part2_primer

import scala.concurrent.Future

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

object FirstPrinciples extends App {

  implicit val system: ActorSystem = ActorSystem("FirstPrinciples")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  // sources
  val source = Source(1 to 10)
  // sinks
  val sink = Sink.foreach[Int](println)

  val graph = source.to(sink)
//  graph.run()


  // flows transform elements
  val flow = Flow[Int].map(x => x + 1)
  val sourceWithFlow = source.via(flow)
  val flowWithSink = flow.to(sink)

//  sourceWithFlow.to(sink).run()
//  source.to(flowWithSink).run()
//  source.via(flow).to(sink).run()

  // nulls are NOT ALLOWED

//  val illegalSource = Source.single[String](null)
//  illegalSource.to(Sink.foreach(println)).run()
  // use Options instead


  val finiteSource = Source.single(1)
  val anotherFiniteSource = Source(List(1,2,3))
  val emptySource = Source.empty[Int]
  val infiniteSource = Source(Stream.from(1)) // dp nt confuse Akka stream with a "collection" Stream

  import scala.concurrent.ExecutionContext.Implicits.global
  val futureSource = Source.fromFuture(Future(42))

  // Sinks
  val theMostBoringSink = Sink.ignore
  val foreachSink = Sink.foreach[String](println)
  val headSink = Sink.head[Int] // retrieves head and then closes the stream
  val foldSink = Sink.fold[Int, Int](0)((a,b) => a+b)


  // flows
  val mapFlow = Flow[Int].map(x => 2 * x)
  val takeFlow = Flow[Int].take(5)
  // drop, filter
  // NOT have flatMap

  val doubleFlowGraph = source.via(mapFlow).via(takeFlow).to(sink)
//  doubleFlowGraph.run()

  // syntactic sugars
  val mapSource = Source(1 to 10).map(x => x*2) // Source(1 to 10).via(Flow[Int].map(x => x*2))

  // run streams directly
  // mapSource.runForeach(println) // map.to(Sink.foreach[Int](println)).run()

  // OPERATORS = components

  /**
   * Exercise: Create a stream that takes the names of persons, then you will keep the first 2 names with length > 5 characters
   */
  val names = List("A big Name", "short1", "", "" , "")

  Source[String](names)
    .filter(_.length > 5)
    .take(2)
    .runForeach(println)

}
