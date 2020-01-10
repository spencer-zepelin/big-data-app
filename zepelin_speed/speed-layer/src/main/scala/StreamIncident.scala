import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamIncident {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  
  // Use the following two lines if you are building for the cluster 
  hbaseConf.set("hbase.zookeeper.quorum","mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal")
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("zepelin_hbase_yearly_combos_speed"))
  val table2 = hbaseConnection.getTable(TableName.valueOf("zepelin_hbase_crimes_by_day_speed"))

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamIncident")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("zepelin-incident")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val serializedRecords = messages.map(_._2);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[IncidentReport]))  
    // How to write to an HBase table



  def addIncident(reports : IncidentReport) : String = {
    // increment the first table
    val inc = new Increment(Bytes.toBytes("2019"))
    inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("totalincidents"), 1L)
    if (reports.combo == "LWLTLP"){
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("LWLTLP"), 1L)
    } else if (reports.combo == "LWLTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("LWLTHP"), 1L)
    } else if (reports.combo == "LWMTLP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("LWMTLP"), 1L)
    } else if (reports.combo == "LWMTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("LWMTHP"), 1L)
    } else if (reports.combo == "LWHTLP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("LWHTLP"), 1L)
    } else if (reports.combo == "LWHTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("LWHTHP"), 1L)
    } else if (reports.combo == "MWLTLP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("MWLTLP"), 1L)
    } else if (reports.combo == "MWLTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("MWLTHP"), 1L)
    } else if (reports.combo == "MWMTLP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("MWMTLP"), 1L)
    } else if (reports.combo == "MWMTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("MWMTHP"), 1L)
    } else if (reports.combo == "MWHTLP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("MWHTLP"), 1L)
    } else if (reports.combo == "MWHTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("MWHTHP"), 1L)
    } else if (reports.combo == "HWLTLP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("HWLTLP"), 1L)
    } else if (reports.combo == "HWLTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("HWLTHP"), 1L)
    } else if (reports.combo == "HWMTLP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("HWMTLP"), 1L)
    } else if (reports.combo == "HWMTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("HWMTHP"), 1L)
    } else if (reports.combo == "HWHTLP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("HWHTLP"), 1L)
    } else if (reports.combo == "HWHTHP") {
      inc.addColumn(Bytes.toBytes("number"), Bytes.toBytes("HWHTHP"), 1L)
    } 
    table.increment(inc)
    val first_return = "COMBO TABLE -- Added incident with weather: " + reports.combo + "\n"
  
    // increment the second table
    val inc2 = new Increment(Bytes.toBytes("2019"))
    inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("allincidents"), 1L)
    if (reports.arrest){
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("allarrests"), 1L)
    } 
    if (reports.wind == "LW") {
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("lowwindincidents"), 1L)
      if (reports.arrest){
        inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("lowwindarrests"), 1L)
      } 
    } else if (reports.wind == "MW") {
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("moderatewindincidents"), 1L)
      if (reports.arrest){
        inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("moderatewindarrests"), 1L)
      } 
    } else if (reports.wind == "HW") {
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("highwindincidents"), 1L)
      if (reports.arrest){
        inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("highwindarrests"), 1L)
      } 
    }
    if (reports.temp == "LT") {
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("lowtempincidents"), 1L)
      if (reports.arrest){
        inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("lowtemparrests"), 1L)
      } 
    } else if (reports.temp == "MT") {
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("moderatetempincidents"), 1L)
      if (reports.arrest){
        inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("moderatetemparrests"), 1L)
      } 
    } else if (reports.temp == "HT") {
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("hightempincidents"), 1L)
      if (reports.arrest){
        inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("hightemparrests"), 1L)
      } 
    }
    if (reports.precip == "LP") {
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("lowprecipincidents"), 1L)
      if (reports.arrest){
        inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("lowpreciparrests"), 1L)
      } 
    } else if (reports.precip == "HP"){
      inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("highprecipincidents"), 1L)
      if (reports.arrest){
        inc2.addColumn(Bytes.toBytes("year"), Bytes.toBytes("highpreciparrests"), 1L)
      } 
    }
    table2.increment(inc2)
    val second_return = "RATE TABLE -- Added incident with weather: " + reports.combo + " and Arrest: " + reports.arrest
    return first_return + second_return;
  }

  val incidentAdded = reports.map(addIncident)
  incidentAdded.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
