package org.example

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag

object FlinkDemo {

  def main(args: Array[String]): Unit = {
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val outputTag = OutputTag[String]("side-output")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    val kafkaTopicRead = "flinkkafkademo"
    val kafkaTopicWrite = "flinkkafkademowrite"
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test-consumer-group")

    val consumer = new FlinkKafkaConsumer[String](
      kafkaTopicRead,
      new StringDeserializationSchema,
      properties)

    val stream = env.addSource(consumer).process(new FlinkProcessFunction)

    // Read Data from Kafka and write in file
    val filePath = "/Users/kartheek/Documents/Workspace/playground/scala/flinkdemo/src/main/resources/streamData.txt"
    stream.writeAsText(filePath, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    // output from process function
    val sideOutputStream = stream.getSideOutput(outputTag)

    val producer = new FlinkKafkaProducer[String](
      kafkaTopicWrite,
      new StringSerializationSchema(kafkaTopicWrite),
      properties,
      Semantic.AT_LEAST_ONCE)

    sideOutputStream.addSink(producer)
    
    env.execute("Flink Kafka Demo Example ")
  }

}
