package com.example

import akka.actor.{ActorKilledException, ActorSelection, ActorSystem}
import akka.testkit.{EventFilter, ImplicitSender, TestKit}
import com.example.ShardWorker.Value
import com.example.ShardedKvStore._
import com.typesafe.config.ConfigFactory
import org.scalatest._

class ShardedKPOSSpec(_system: ActorSystem) extends TestKit(_system)
                                                with WordSpecLike
                                                with MustMatchers
                                                with BeforeAndAfterAll
                                                with ImplicitSender {


  def this() =
    this(ActorSystem("POSLoadData",
                      ConfigFactory.parseString(
                        """akka.loggers = ["akka.testkit.TestEventListener"]"""
                                               )))


  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "POS Load Events" must {
    val kvStoreActor = system.actorOf(ShardedKvStore.props,
      "ScalaProject")

    kvStoreActor ! Initialize(3)


    "Load data into queue" in {
        kvStoreActor ! Load("fookey")
      }


    "Read data from queue" in {
        kvStoreActor ! Read("foo")
    }
  }

}
