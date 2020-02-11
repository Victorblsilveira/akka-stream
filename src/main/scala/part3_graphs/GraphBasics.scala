package part3_graphs

import scala.collection.mutable

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}

object GraphBasics extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("GraphBasics")
  implicit val materializer: Materializer = ActorMaterializer()

  val input = Source(1 to 1000)
  val incrementer = Flow[Int].map(x => x + 1) // hard computation
  val multiplier = Flow[Int].map(x => x * 10) // hard computation
  val output = Sink.foreach[(Int, Int)](println)

  // step 1 - setting up the fundamentals fro the graph
  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] => // Builder // MUTABLE data structure

      import GraphDSL.Implicits._ // Brings nice operators into scope

      // step 2 - add the necessary components to this graph
      val broadcast = builder.add(Broadcast[Int](2)) // fan-out operator, one input many outputs
      val zip = builder.add(Zip[Int, Int]) // fan-in operator, multiple inputs one output

      // step 3 - tying up the components
      input ~> broadcast

      broadcast.out(0) ~> incrementer ~> zip.in0
      broadcast.out(1) ~> multiplier  ~> zip.in1

      zip.out ~> output

      // step 4 - return a closed shaped
      ClosedShape // FREEZE the builder's shape
      // shape
    } // Graph
  ) // runnable graph

  // graph.run() // run the graph and materialize it


  /**
   * exercise 1: feed a source into 2 sinks at the same time  (hint:  use broadcast)
   * */
  val source = Source(1 to 100)
  val source2Sinks = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>

      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[Int](2))

      // implicit port allocation (allocates ports in order)
      source ~> broadcast ~> Sink.foreach[Int](println)
                broadcast ~> Sink.foreach[Int](x => println(x*2))

      ClosedShape

    }
  )

//  source2Sinks.run()

  /**
   * exercise 2: Balance
   * */
  import scala.concurrent.duration._

  val fastSource = Source(1 to 1000).throttle(5, 1 second)
  val slowSource = Source(1 to 1000).throttle(2, 1 second)
  val sink1 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 1: number of elements $count")
    count + 1
  })

  val sink2 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 1: number of elements $count")
    count + 1
  })


  val balanceGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>

      import GraphDSL.Implicits._

      val merge = builder.add(Merge[Int](2))

      // Distribute the inputs to the outputs
      val balance = builder.add(Balance[Int](2))

      // implicit port allocation (allocates ports in order)
      fastSource ~> merge
      slowSource ~> merge

      merge ~> balance ~> sink1
               balance ~> sink2

      ClosedShape

    }
  )

  balanceGraph.run()
}

