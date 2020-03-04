package part3_graphs

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape, Materializer, SinkShape, SourceShape}

object OpenGraphs extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("OpenGraphs")
  implicit val materializer: Materializer = ActorMaterializer()

  /*
    A composite source that concatenates 2 sources
    - Emits ALL element from the first source
    - then ALL the elements from the second
  */

  val firstSource = Source(1 to 10)
  val secondSource = Source(42 to 1000)

  // Step 1
  val sourceGraph = Source.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2: declaring components
      val concat = builder.add(Concat[Int](2))

      firstSource ~> concat
      secondSource ~> concat

      // step3: tying them together

      // Step 4
      SourceShape(concat.out)
    }
  )

  //  sourceGraph.to(Sink.foreach(println)).run()

  /*
    Complex Sink
  */

  val sink1 = Sink.foreach[Int](x => println(s"Meanigful thing 1: $x"))
  val sink2 = Sink.foreach[Int](x => println(s"Meanigful thing 2: $x"))

  val sinkGraph = Sink.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2 -- add broadcast

      val broadcast = builder.add(Broadcast[Int](2))

      // step 3 -- tie components together
      broadcast ~> sink1
      broadcast ~> sink2

      SinkShape(broadcast.in)
    }
  )

  firstSource.to(sinkGraph).run()


  /**
   * Challenge - complex flow ?
   * Write you own flow that's composed of two other flows
   * - one that adds 1 to a number
   * - one that dos number *10
   */
  val incrementer = Flow[Int].map(_ + 1)
  val multiplier = Flow[Int].map(_ * 10)
  val flowGraph = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      //  everything operates on SHAPES
      // step 2 - define auxiliary SHAPES
      val incrementerShape = builder.add(incrementer)
      val multiplierShaper = builder.add(multiplier)

      // step 3 - connect the SHAPES
      incrementerShape ~> multiplierShaper

      FlowShape(incrementerShape.in, multiplierShaper.out)
    } // Static Graph
  ) // Component

  firstSource.via(flowGraph).to(Sink.foreach(println)).run()

  /**
   * Exercise: flow from a sink and a source ?
   **/


  // 1
  def fromSinkAndSource[A, B](sink: Sink[A, _], source: Source[B, _]): Flow[A, B, _] =
  // Step 1
    Flow.fromGraph(
      GraphDSL.create() { implicit builder =>
        // step 2 declare the SHAPES
        val sourceShape = builder.add(source)
        val sinkShape = builder.add(sink)

        // Step 3
        // step 4 - return the shape
        FlowShape(sinkShape.in, sourceShape.out)

      }
    )

  val simpleSink = Sink.foreach(println)
  val simpleSource = Source.empty

  Flow.fromSinkAndSource(simpleSink, simpleSource)

  // Coupled means if the sink stops the source will be stopped to

  Flow.fromSinkAndSourceCoupled(simpleSink, simpleSource)


}
