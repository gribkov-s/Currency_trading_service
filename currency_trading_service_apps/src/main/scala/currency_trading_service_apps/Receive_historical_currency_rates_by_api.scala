package currency_trading_service_apps


import org.apache.log4j.PropertyConfigurator

import java.time.LocalDate._

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Receive_historical_currency_rates_by_api extends App {


  val spark = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  spark.sparkContext.setLogLevel("ERROR")
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

  var prev_days_qty = args(0).toInt

  val end_date = now()

  var begin_date = end_date.minusDays(prev_days_qty)

  var compare_result = end_date.compareTo(begin_date)

  val http_client = new DefaultHttpClient()


  while (compare_result >= 0) {

    val http_get = new HttpGet("https://currencyscoop.p.rapidapi.com/historical?base=RUB&date=" + begin_date)
    http_get.setHeader("x-rapidapi-host", "currencyscoop.p.rapidapi.com")
    http_get.setHeader("x-rapidapi-key", "69583006aemshc1e88da72ef736fp123742jsnce80e709be41")

    val http_response = http_client.execute(http_get)

    val entity = http_response.getEntity //



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

    val json_data = EntityUtils.toString(entity)
    print(json_data)


    val data = spark.read.schema(expected_schema).json(Seq(json_data).toDS)
      .select($"response.date", explode($"response.rates"))
      .join(broadcast(dim), 'key === 'currency_code)
      .withColumn("rate", pow($"value", -1) * $"scale")
      .withColumnRenamed("response.date","date")
      .withColumnRenamed("key","currency")
      .select($"date", $"currency", $"rate")

    data.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/currency_trading")
      .option("dbtable", "trading.currency_rates_daily")
      .option("user", "postgres")
      .option("password", "12345")
      .option("driver", "org.postgresql.Driver")
      .mode("append")
      .save()

    begin_date = begin_date.plusDays(1)
    compare_result = end_date.compareTo(begin_date)

  }

  http_client.getConnectionManager.shutdown() //

}