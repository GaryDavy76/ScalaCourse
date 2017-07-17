package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.example.ShardWorker.Stun

/**
  * Akka Actor representing the world facing component of a distributed
  * key-value store.
  */
object ShardedKvStore {
  val props = Props[ShardedKvStore]

  /////////////////////////////////////////////////////////////////////////////
  // Messages for the key-value store's user facing protocol
  /////////////////////////////////////////////////////////////////////////////
  case class Initialize(numShards: Int)
  case class Read(key: String)
  case class Load(key: String)
  case class Size(size: Int, numNonRespondingShards: Int)
  case object StunWorker // Test/debug message to stun first shard worker.
  case class VandalizeShard(num: Int) // Test/debug message to test lifecycle.
}


class ShardedKvStore extends Actor with ActorLogging {
  import ShardedKvStore._

  // Create as our child a supervisor of the shards...
  val shardSupervisor = context.actorOf(ShardSupervisor.props,
                                        "ShardSupervisor")


  override def receive: Receive = {

    case Initialize(n) => shardSupervisor forward Initialize(n)
    case Read(key) => shardSupervisor forward Read(key)
    case Load(key) => shardSupervisor forward Load(key)
    case StunWorker => shardSupervisor forward StunWorker

    case VandalizeShard(n) => shardSupervisor forward VandalizeShard(n)

    case m@_ => log.warning(s"NARB! Unexpected message $m")
  }
}

