package currency_trading_service_apps


import org.apache.log4j.PropertyConfigurator

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._

object Kafka_Consumer_read_currency_rates_from_topic extends App {

  val log4j_conf_path = System.getProperty ("user.dir") + "/src/main/resources/log4j.properties"
  PropertyConfigurator.configure(log4j_conf_path)

  val spark = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  import spark.sqlContext.implicits._

  /////////////////////////////////////////////////////////////////////////

  val dim = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/currency_trading")
    .option("dbtable", "trading.dim_currency")
    .option("user", "postgres")
    .option("password", "12345")
    .option("driver", "org.postgresql.Driver")
    .load()
    .filter('is_basic === false)
    .withColumnRenamed("currency","currency_code")
    .select($"currency_code", $"scale")

  //////////////////////////////////////////////////////////////////////////

  val input_stream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "currency_rates_rub")
    .option("startingOffsets", "latest")
    .load()

  val expected_schema = new StructType()
    .add("meta", new StructType()
      .add("code", IntegerType)
      .add("disclaimer", StringType)
    )
    .add("response", new StructType()
      .add("date", TimestampType)
      .add("base", StringType)
      .add("rates", MapType(StringType, FloatType))
    )

  val data = input_stream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json($"value", expected_schema).as("data")).select("data.response.*")
    .select($"date", explode($"rates"))
    .join(broadcast(dim), 'key === 'currency_code)
    .withColumn("rate", pow($"value", -1) * $"scale")
    .withColumnRenamed("date","datetime")
    .withColumnRenamed("key","currency")
    .select($"datetime", $"currency", $"rate")

  ////////////////////////////////////////////////////////////////////////////

  var interval_sec = args(0).toInt

  data.writeStream.foreachBatch {
    (dataset: DataFrame, batchId: Long) =>

      dataset
        .select($"datetime", $"currency", $"rate")
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/currency_trading")
        .option("dbtable", "trading.currency_rates_current")
        .option("user", "postgres")
        .option("password", "12345")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()

  }
    .trigger(Trigger.ProcessingTime(interval_sec.toString + " seconds"))
    .start()
    .awaitTermination()

}
