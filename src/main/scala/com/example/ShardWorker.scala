package com.example

import java.util.{Collections, Date, Properties, UUID}

import akka.actor.{Actor, ActorLogging, Props}
import com.example.ShardWorker._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.immutable.HashMap
import scala.util.Random
import java.util.concurrent._
import kafka.consumer.KafkaStream
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import com.google.gson.Gson
import java.sql.{Connection,DriverManager}

import scala.collection.JavaConversions._


object ShardWorker {
  val props = Props[ShardWorker]

  case class Value(key: String, value: Option[String]) // Return queried value.
  case class ShardSize(n: Int)      // Report number of values stored on shard.
  case class SalesOrder(TransactionNumber: Int, StoreNumber: Int, SkuNumber: Int, TransactionType: String, Amount: Int, TransactionTime: String )

  case object Stun    // Debug/test message: stuns shard into unresponsiveness.
  case object BlowYourselfUp // Debug/test message: makes worker throw.
}


class ShardWorker extends Actor with ActorLogging {
  import ShardedKvStore._

  var kvStore = new HashMap[String, String]()


  override def receive: Receive = {

    case Read(k) =>
      log.info("Gary Delete ")
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

      val consumer = new KafkaConsumer[String, String](props)
      consumer.subscribe(Collections.singletonList("test"))

      try while (true) {
        val records = consumer.poll(10000)
        log.info("Gary Records: " + records.count)
        for (record <- records) {
          log.info("Gary Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
        }
      }
      catch
        {
          case e: Exception => e.printStackTrace()
        }

      shutdown()

      def shutdown() = {
        if (consumer != null)
          consumer.close();
      }

      sender ! Value(k, kvStore.get(k))
    case Load(k) =>
      //sender ! Value(k, kvStore.get(k))
      val topic = "test"
      val brokers = "localhost:9092"

      val rnd = new Random()
      val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("client.id", "ScalaProducerExample")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      //Gson

      val r = scala.util.Random
      // TODO put below values encrypted into a config file of sorts
      val gson = new Gson
      val url = "jdbc:mysql://localhost:3306/scalastore"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "root"
      val producer = new KafkaProducer[String, String](props)
      val t = System.currentTimeMillis()

      Class.forName(driver)
      var connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      val statementup = connection.createStatement

      try
          for (nEvents <- 0 to 9) {
            val runtime = new Date().getTime()
            val ip = "192.168.2." + rnd.nextInt(255)
            val ActualOrder = new SalesOrder(nEvents, r.nextInt(100), r.nextInt(100), "Cash", r.nextInt(50), runtime.toString())
            val data = new ProducerRecord[String, String](topic, ip, gson.toJson(ActualOrder))
            producer.send(data)
            val sqlstring = "INSERT INTO `scalastore`.`salesordermessages` (`Message`) VALUES ( '" + gson.toJson(ActualOrder) + "');"
            val rs = statement.execute(sqlstring)
            val rss = statement.executeQuery("SELECT StoreID, NumberOfTransactions, AmountOfTransactions FROM salesdatabystore WHERE salesdatabystore.StoreID =" + ActualOrder.StoreNumber)
            var count = 0
              while (rss.next) {
                val NoOfTrans = rss.getInt("NumberOfTransactions") + 1
                val AmountOfTrans = rss.getInt("AmountOfTransactions") + ActualOrder.Amount
                val sqlstringupdate = " UPDATE `scalastore`.`salesdatabystore` SET `NumberOfTransactions` = '" + NoOfTrans + "',`AmountOfTransactions` = '" + AmountOfTrans +
                  "' WHERE `StoreID` = '" + ActualOrder.StoreNumber + "';"
                val rsin = statementup.execute(sqlstringupdate)
                count += 1
              }

            if (count == 0) {
              val sqlstringinsert = "INSERT INTO `scalastore`.`salesdatabystore` (`StoreID`, `NumberOfTransactions`, `AmountOfTransactions`) " +
                "VALUES ( '" + ActualOrder.StoreNumber + "','" + ActualOrder.TransactionNumber + "', '" + ActualOrder.Amount + "');"
              val rsup = statement.execute(sqlstringinsert)
            }
          }
      catch {
        case e: Exception => e.printStackTrace()
      }

      log.info("sent per second: " + 1 / (System.currentTimeMillis() - t))
      producer.close()
      connection.close

      sender ! Value(k, kvStore.get(k))

    case Stun => context.become(stunned)
    case BlowYourselfUp => throw new IllegalStateException(s"$self has been made fatally unhappy")
  }

  /**
    * A Receive behavior for when this ShardWorker has been stunned.  It
    * permanently becomes unable to respond to messages with useful work,
    * and only logs that it received them.  We use this stunned state to
    * test one of this week's exercises.
    *
    * @return
    */

  def stunned: Receive = {
    case msg@_ =>  log.info(s"!!! Stun actor is permanently useless )")
  }

  ////////////////////////////////////////////////////////////////////////////
  // Lifecycle hooks to override for illustrative purposes.
  ////////////////////////////////////////////////////////////////////////////
  override def preStart(): Unit = log.info(s"!!! preStart )")
  override def postStop(): Unit = log.info(s"!!! postStop )")
  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    log.info(s"!!! preRestart )")
  override def postRestart(reason: Throwable): Unit = log.info(s"!!! postRestart )")
}
