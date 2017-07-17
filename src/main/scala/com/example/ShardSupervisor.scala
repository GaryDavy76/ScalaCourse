package com.example

import akka.actor.SupervisorStrategy.{Decider, Restart, Stop}
import akka.actor.{Actor, ActorInitializationException, ActorKilledException, ActorLogging, ActorRef, OneForOneStrategy, PoisonPill, Props}
import com.example.ShardWorker.{BlowYourselfUp, Stun}
import com.example.ShardedKvStore._

import scala.concurrent.duration._
import scala.language.postfixOps

object ShardSupervisor {
  val props = Props[ShardSupervisor]
}

class ShardSupervisor extends Actor with ActorLogging {

  var numShards = 0
  var shardWorkers: Vector[ActorRef] = scala.collection.immutable.Vector.empty

  override final val supervisorStrategy = OneForOneStrategy (maxNrOfRetries = 2, withinTimeRange = 1 second)
  {
    case _: ActorInitializationException      => Stop
    case _: ActorKilledException              => Restart
    case _: Exception                         => Restart
  }

  def bucketNumber(key: String): Int = (key.hashCode & 0x7fffffff) % numShards

  override def receive: Receive = {

    case Initialize(n) =>
      require(n > 0)
      numShards = n
      for ( i <- 0 until n ) {
        shardWorkers = shardWorkers :+ context.actorOf(ShardWorker.props, s"Shardworker-for-$this-$i")}
    case Read(key) =>
      val bucket = bucketNumber(key)
      shardWorkers(bucket) ! Read(key)
    case Load(key) =>
      val bucket = bucketNumber(key)
      shardWorkers(bucket) forward Load(key)
    case StunWorker=> shardWorkers(0) forward Stun
    case VandalizeShard(n) => shardWorkers(n) !  BlowYourselfUp
    case m@_ => log.warning(s"NARB! Unexpected message $m")
  }

}
