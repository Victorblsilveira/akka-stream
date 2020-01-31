import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}

object Playground extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("Udemy-Akka-Streams")
  implicit val materializer: Materializer = ActorMaterializer()

  Source
    .single("Hello, Streams!")
    .to(Sink.foreach(println))
    .run

}
