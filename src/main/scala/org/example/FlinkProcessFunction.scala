package org.example

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala.OutputTag

class FlinkProcessFunction extends ProcessFunction[String, String]{
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[FlinkProcessFunction])
  val outputTag = OutputTag[String]("side-output")

  override def processElement(streamData: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
    logger.info("before streamData "+ streamData)

    val data= streamData.split(" ").groupBy(identity)
    val dataWithCounts = data.mapValues(_.size)
    logger.info("after data "+ data)
    logger.info("after dataWithCounts "+ dataWithCounts)

    context.output(outputTag, String.valueOf(dataWithCounts))
  }
}
