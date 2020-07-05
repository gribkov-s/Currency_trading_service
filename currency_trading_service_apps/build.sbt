resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

name := "currency_trading_service_apps"

version := "0.0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(

      "org.slf4j" % "slf4j-api" % "1.7.30",
      "org.slf4j" % "slf4j-log4j12" % "1.7.30",

      "org.apache.spark" %% "spark-sql" % "2.4.0",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
      "org.apache.spark" %% "spark-mllib" % "2.4.0",
      "org.postgresql" % "postgresql" % "42.2.10",

      "mrpowers" % "spark-daria" % "0.35.2-s_2.11",
      "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11" % "test",
      "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)


// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// JAR file settings
assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
}

// don't include Scala in the JAR file
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
// You can add the JAR file naming conventions by running the shell script
