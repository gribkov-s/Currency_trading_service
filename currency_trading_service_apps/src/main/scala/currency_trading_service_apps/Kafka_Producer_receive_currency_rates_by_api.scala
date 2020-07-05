package currency_trading_service_apps


import org.apache.log4j.PropertyConfigurator

import java.util.Properties

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.kafka.clients.producer._
import org.apache.spark.sql.functions._


object Kafka_Producer_receive_currency_rates_by_api extends App {

  val log4j_conf_path = System.getProperty ("user.dir") + "/src/main/resources/log4j.properties"
  PropertyConfigurator.configure(log4j_conf_path)

  var interval_sec = args(0).toInt


  while (1 == 1 ) {

    val http_client = new DefaultHttpClient()

    val http_get = new HttpGet("https://currencyscoop.p.rapidapi.com/latest?base=RUB")
    http_get.setHeader("x-rapidapi-host", "currencyscoop.p.rapidapi.com")
    http_get.setHeader("x-rapidapi-key", "69583006aemshc1e88da72ef736fp123742jsnce80e709be41")

    val http_response = http_client.execute(http_get)

    val entity = http_response.getEntity ////

    val data = EntityUtils.toString(entity)

    http_client.getConnectionManager.shutdown() ////

    println(data)

    val  props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val record = new ProducerRecord("currency_rates_rub", current_timestamp().toString, data)
    producer.send(record)


    Thread.sleep(interval_sec * 1000)

  }

}