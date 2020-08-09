package currency_trading_service_apps


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object ML_model_currency_rate_predict extends App {


  val spark = {
    SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  spark.sparkContext.setLogLevel("ERROR")
  import spark.sqlContext.implicits._

  ////////////////////////////////////////////////////////////////////

  val data = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/currency_trading") //localhost
    .option("dbtable", "trading.currency_rates_current_hourly")
    .option("user", "postgres")
    .option("password", "12345")
    .option("driver", "org.postgresql.Driver")
    .load()

  val training_data = data
    .select($"date_hour", $"currency_id", $"currency", $"rate")
    .withColumn("datetime_unix", unix_timestamp($"date_hour"))


  val dim = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/currency_trading")
    .option("dbtable", "trading.dim_currency")
    .option("user", "postgres")
    .option("password", "12345")
    .option("driver", "org.postgresql.Driver")
    .load()
    .filter('is_basic === false)
    .withColumnRenamed("id", "currency_id")

  val currency_count = dim.count().toInt

  val data_for_predict = dim
    .withColumn("datetime_current_unix", unix_timestamp(current_timestamp()))
    .withColumn("datetime_unix", floor($"datetime_current_unix"/3600)*3600+3600) 
    .withColumn("datetime", to_timestamp(from_unixtime($"datetime_unix")))
    .withColumnRenamed("currency_code", "currency")
    .select($"datetime", $"currency", $"datetime_unix", $"currency_id")

  //////////////////////////////////////////////////////////////////////

  val assembler = new VectorAssembler()
    .setInputCols(Array("datetime_unix", "currency_id"))
    .setOutputCol("features")

  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(currency_count)

  val dt = new DecisionTreeRegressor()
    .setLabelCol("rate")
    .setFeaturesCol("indexedFeatures")

  /////////////////////////////////////////////////////////////////////

  val pipeline = new Pipeline()
      .setStages(Array(assembler, featureIndexer, dt))

  val model = pipeline.fit(training_data)

  val predictions = model.transform(data_for_predict)


  val output_data = predictions
      .withColumnRenamed("datetime", "datetime_hour")
      .withColumn("datetime", current_timestamp())
      .withColumnRenamed("prediction", "rate")
      .select($"datetime", $"currency", $"rate", $"datetime_hour")

  output_data.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/currency_trading")
    .option("dbtable", "trading.currency_rates_predicted")
    .option("user", "postgres")
    .option("password", "12345")
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()

}
