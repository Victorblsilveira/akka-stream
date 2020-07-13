package part5_advanced

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Graph, Inlet, Outlet, Shape}

import scala.concurrent.duration._
import scala.collection.immutable

object CustomGraphShapes extends App {

  implicit val system = ActorSystem("CustomGraphShapes")
  implicit val materializer = ActorMaterializer()

  // Balance 2x3 shape
  case class Balance2x3 (
    in0: Inlet[Int],
    in1: Inlet[Int],
    out0: Outlet[Int],
    out1: Outlet[Int],
    out2: Outlet[Int]) extends Shape {

    // Inlet[T], Outlet[T]
    override def inlets: immutable.Seq[Inlet[_]] = List(in0, in1)
    override def outlets: immutable.Seq[Outlet[_]] = List(out0, out1, out2)

    override def deepCopy(): Shape = Balance2x3(
      in0.carbonCopy(),
      in1.carbonCopy(),
      out0.carbonCopy(),
      out1.carbonCopy(),
      out2.carbonCopy()
    )
  }

  val balance2x3Impl = GraphDSL.create(){ implicit builder =>
    import GraphDSL.Implicits._

    val merge = builder.add(Merge[Int](2))
    val balance = builder.add(Balance[Int](3))

    merge ~> balance

    Balance2x3(
      merge.in(0),
      merge.in(1),
      balance.out(0),
      balance.out(1),
      balance.out(2)
    )
  }

  val balance2x3Graph = RunnableGraph.fromGraph(
    GraphDSL.create(){ implicit builder =>
      import GraphDSL.Implicits._

      val slowSource = Source(Stream.from(1)).throttle(1, 1 second)
      val fastSource = Source(Stream.from(1)).throttle(2, 1 second)

      def createSink(index: Int) = Sink.fold(0)((count: Int, element: Int) => {
      println(s"[sink index $index] Received $element, current count is $count")
        count + 1
    })

      val sink1 = builder.add(createSink(1))
      val sink2 = builder.add(createSink(2))
      val sink3 = builder.add(createSink(3))

      val balance2x3 = builder.add(balance2x3Impl)

      slowSource ~> balance2x3.in0
      fastSource ~> balance2x3.in1

      balance2x3.out0 ~> sink1
      balance2x3.out1 ~> sink2
      balance2x3.out2 ~> sink3

      ClosedShape
    }
  )

  balance2x3Graph.run()

  /**
   * Excercise: Generalize the balance component, make it M x N
   *
   * */

  // Generic an Unlimited Balance shape
  case class BalanceMxN[+T](inlet: immutable.Seq[Inlet[T]],
                            outlet: immutable.Seq[Outlet[T]]
                           ) extends Shape {

    // Inlet[T], Outlet[T]
    override def inlets: immutable.Seq[Inlet[_]] = inlet
    override def outlets: immutable.Seq[Outlet[_]] = outlet

    override def deepCopy(): Shape = BalanceMxN(
      inlet.map(_.carbonCopy()),
      outlet.map(_.carbonCopy())
    )
  }

  object BalanceMxN {
    def apply[T](inputs: Int, outputs: Int): Graph[BalanceMxN[T], NotUsed] =
      GraphDSL.create(){ implicit builder =>
      import GraphDSL.Implicits._

      val merge = builder.add(Merge[T](inputs))
      val balance = builder.add(Balance[T](outputs))

      merge ~> balance

      BalanceMxN(merge.inlets, balance.outlets)
    }
  }

  val balanceMxNGraph = RunnableGraph.fromGraph(
    GraphDSL.create(){ implicit builder =>
      import GraphDSL.Implicits._

      val slowSource = Source(Stream.from(1)).throttle(1, 1 second)
      val fastSource = Source(Stream.from(1)).throttle(2, 1 second)

      def createSink(index: Int) = Sink.fold(0)((count: Int, element: Int) => {
        println(s"[sink index $index] Received $element, current count is $count")
        count + 1
      })

      val sink1 = builder.add(createSink(1))
      val sink2 = builder.add(createSink(2))
      val sink3 = builder.add(createSink(3))

      val balanceMxN = builder.add(BalanceMxN[Int](2,3))

      slowSource ~> balanceMxN.inlet(0)
      fastSource ~> balanceMxN.inlet(1)

      balanceMxN.outlet(0) ~> sink1
      balanceMxN.outlet(1) ~> sink2
      balanceMxN.outlet(2) ~> sink3

      ClosedShape
    }
  )


}
