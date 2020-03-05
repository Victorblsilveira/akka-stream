package part3_graphs

import java.util.Date

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, ZipWith}
import akka.stream.{ActorMaterializer, ClosedShape, FanOutShape2, Materializer, UniformFanInShape}

object MoreOpenGraphs extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("MoreOpenGraphs")
  implicit val materializer: Materializer = ActorMaterializer()


  /* Examṕle: Max3 operator
  * - 3 inputs of type int
  * - the maximum of the 3
  * */

  val max3StaticGraph = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._

      // step 2 - define aux SHAPES

      val max1 = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))
      val max2 = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))

      // step 3
      max1.out ~> max2.in0

      // step 4
      UniformFanInShape(max2.out, max1.in0, max1.in1, max2.in1)
  }

  val source1 = Source(1 to 10)
  val source2 = Source(1 to 10).map(_ => 5)
  val source3 = Source((1 to 10).reverse)

  val maxSink =  Sink.foreach[Int]( x => println(s"Max is : $x"))

  val max3RunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit  builder =>
      import GraphDSL.Implicits._

      // step 2 - Declare SHAPES
      val max3Shapes = builder.add(max3StaticGraph)

      // step 3 - tie
      source1 ~> max3Shapes.in(0)
      source2 ~> max3Shapes.in(1)
      source3 ~> max3Shapes.in(2)
      max3Shapes.out ~> maxSink

      // Step 4
      ClosedShape
    }
  )

//  max3RunnableGraph.run()

  // Same for UniformFanOutShape


  /* Non-Uniform fan out shape
  * Processing bank transactions :
  * Txn suspicious if amount > 10000
  *
  * Streams component for txns
  * - output1: let the transaction go through
  * - output2: suspicious txn ids
  * */

  case class Transaction(id: String, source: String, recipient: String, amount: Int, date: Date)

  val transactionSOurce = Source(List(
    Transaction("123123123", "Paul", "Jim", 100, new Date),
    Transaction("879864514", "Daniel", "Jim", 100000, new Date),
    Transaction("897651321", "Daniel", "Alice", 7000, new Date)
  ))

  val bankProcessor = Sink.foreach[Transaction](println)
  val suspiciousAnalysisService = Sink.foreach[String](txnId => println(s"Suspicious transaction ID: $txnId"))

  val suspiciousTxnStaticGraph = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    // step 2 - define SHAPES
    val broadcast = builder.add(Broadcast[Transaction](2))
    val suspiciousTxnFilter = builder.add(Flow[Transaction].filter(txn => txn.amount > 10000))
    val txnIdExtractor = builder.add(Flow[Transaction].map(txn => txn.id))


    // strep 3 - tie Shapes
    broadcast.out(0) ~> suspiciousTxnFilter ~> txnIdExtractor

    // step 4
    new FanOutShape2(broadcast.in, broadcast.out(1), txnIdExtractor.out)
  }

  val suspiciousTxRunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2
      val suspicioutTxnSHape = builder.add(suspiciousTxnStaticGraph)


      // step 3
      transactionSOurce ~> suspicioutTxnSHape.in
      suspicioutTxnSHape.out0 ~> bankProcessor
      suspicioutTxnSHape.out1 ~> suspiciousAnalysisService

      ClosedShape
    }
  )

  suspiciousTxRunnableGraph.run()

}
