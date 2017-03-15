package homework


import java.util.{Calendar, Date, Properties, UUID}

import org.apache.spark.streaming.{Seconds, StateSpec, StreamingContext, Time}
import org.apache.spark._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.hive.HiveContext

object Homework3 {
  private final val TimestampFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private var topic: String = _
  private var settings: Map[String, BytesPerPeriodInSec] = _
  private var producer: KafkaProducer[String, String] = _

  object StateEnum extends Enumeration {
    type StateEnum = Value
    val Exceeded, Norm = Value
  }

  import StateEnum._

  case class SimpleState(var state: StateEnum, var valueInBytes: Long, var timestampInMillis: Long)

  case class State(var limit: SimpleState, var threshold: SimpleState)

  case class BytesPerPeriodInSec(val bytesPerPeriod: Long, val periodInSec: Long)

  case class Record(recordTime: Long, srcIP: String, capLen: Long)

  def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: org.apache.spark.streaming.State[State]): Option[(String, State)] = {
    val state_ = state.getOption().getOrElse(new State(new SimpleState(Norm, 0, batchTime.milliseconds), new SimpleState(Norm, 0, batchTime.milliseconds)))
    val averageSpeedFromSettings = settings.getOrElse(key + "+1", settings.get("null+1").get)
    val limitFromSettings = settings.getOrElse(key + "+2", settings.get("null+2").get)

    state_.limit.valueInBytes += value.getOrElse(0)
    if (batchTime.milliseconds - state_.limit.timestampInMillis > limitFromSettings.periodInSec * 1000) {
      if (state_.limit.valueInBytes > limitFromSettings.bytesPerPeriod) {
        if (state_.limit.state == Norm) {
          state_.limit.state == Exceeded
          var alert = new Alert(batchTime.milliseconds, key, 2, state_.limit.valueInBytes, averageSpeedFromSettings.bytesPerPeriod, averageSpeedFromSettings.periodInSec)
          val message = new ProducerRecord[String, String](topic, null, alert.toString)
          producer.send(message)
        }
      } else {
        state_.limit.state == Norm
      }
      state_.limit.timestampInMillis = batchTime.milliseconds
      state_.limit.valueInBytes = 0
    }

    state_.threshold.valueInBytes += value.getOrElse(0)
    if (batchTime.milliseconds - state_.threshold.timestampInMillis > averageSpeedFromSettings.periodInSec * 1000) {
      val bytesPerSec = state_.threshold.valueInBytes / (averageSpeedFromSettings.periodInSec)
      if (bytesPerSec > averageSpeedFromSettings.bytesPerPeriod) {
        if (state_.threshold.state == Norm) {
          state_.threshold.state == Exceeded
          var alert = new Alert(batchTime.milliseconds, key, 1, bytesPerSec, averageSpeedFromSettings.bytesPerPeriod, averageSpeedFromSettings.periodInSec)
          val message = new ProducerRecord[String, String](topic, null, alert.toString)
          producer.send(message)
        }
      } else {
        state_.threshold.state == Norm
      }

      state_.threshold.timestampInMillis = batchTime.milliseconds
      state_.threshold.valueInBytes = 0
    }

    state.update(state_)
    return Some(key, state_)
  }

  def main(args: Array[String]) {
    val c = new Properties
    c.load(this.getClass.getResourceAsStream("/receiver.properties"))
    topic = c.getProperty("topic", "alerts").asInstanceOf[String]
    val brokers = c.getProperty("broker", "localhost:6667").asInstanceOf[String]
    val monitoringPeriodInSec = c.getProperty("monitoringPeriodInSec", "10").asInstanceOf[String].toLong
    val windowSizeInSec = c.getProperty("windowSizeInSec", "60*60").asInstanceOf[String].toLong

    val props = new java.util.HashMap[String, Object]
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    producer = new KafkaProducer[String, String](props)

    val conf = new SparkConf().setAppName("Homework3").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("/tmp/checkpoint")


    val hiveContext = new HiveContext(sc)
    hiveContext.sql("CREATE TABLE IF NOT EXISTS statistics_by_hour (ts TIMESTAMP, HostIP STRING, TrafficConsumed DOUBLE, AverageSpeed DOUBLE)")

    hiveContext.sql("CREATE TABLE IF NOT EXISTS settings (HostIP STRING, Type INT, Value DOUBLE, Period INT)")
    if (hiveContext.sql("SELECT * FROM settings").collect().length == 0) {
      hiveContext.sql("insert into table settings select t.* FROM (SELECT NULL as HostIP, 1 as Type, 0.0001 as Value, 1 as Period) t")
      hiveContext.sql("insert into table settings select t.* FROM (SELECT NULL as HostIP, 2 as Type, 0.001 as Value, 1 as Period) t")
    }

    settings = hiveContext.sql("SELECT HostIP, Type, Value, Period FROM settings")
      .collect().map(r => ("" + r.getAs("hostip") + "+" + r.getAs("type")) ->
      new BytesPerPeriodInSec(r.getAs("value").asInstanceOf[Number].longValue * 1024 * 1024, r.getAs("period").asInstanceOf[Number].longValue())).toMap

    val customReceiverStream = ssc.receiverStream(new PacketCapturingReceiver())

    customReceiverStream.map(r => (r.src_ip.getHostAddress, r.caplen))
      .reduceByKey((a, b) => a + b)
      .mapWithState(StateSpec.function(trackStateFunc _))
      .print(0)

    val sqlContext = hiveContext
    import sqlContext.implicits._
    customReceiverStream.map(r => (r.src_ip.getHostAddress, r.caplen))
      .reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(windowSizeInSec), Seconds(windowSizeInSec))
      .map(x => (TimestampFormat.format(new Date()), x._1, x._2, x._2 / windowSizeInSec))
      .foreachRDD({
        r => {
          r.toDF().write.mode("append").saveAsTable("statistics_by_hour")
        }
      }
      )

    ssc.start()
    ssc.awaitTerminationOrTimeout(monitoringPeriodInSec * 1000)

    val query = "SELECT hostip, AVG(averagespeed) as av from statistics_by_hour GROUP BY hostip ORDER BY av desc LIMIT 3"
    println
    hiveContext.sql(query).show
    println(s"Query '$query' after monitoring for $monitoringPeriodInSec seconds")
    ssc.stop()
  }

  class Alert(val timestamp: Long, val hostIP: String, val adjustmentType: Int, val factValue: Double, val thresholdValue: Double, val period: Long) {
    val id: UUID = UUID.randomUUID()

    override def toString: String = s"(incident [$id, $hostIP, $adjustmentType, $factValue, $thresholdValue, $period])"
  }


}
