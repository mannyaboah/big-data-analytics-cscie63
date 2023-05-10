package edu.harvard.jobs

import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** This class loads the vertices from the Postgres database into the
  * DSE Graph The vertices are loaded into the DSE Graph using the
  * updateVertices method
  *
  * @author
  *   Manny Aboah
  */
object FlightGraphLoaderV extends App {

  val GRAPH_NAME = "flights"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("FlightGraphLoaderV")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  val g = spark.dseGraph(GRAPH_NAME)

  writeAircraftVertices(spark, g)
  writeAirportsVertices(spark, g)
  writeTicketVertices(spark, g)
  writeFlightVertices(spark, g)

  /** This method loads the aircraft vertices from the Postgres database into
    * the DSE Graph.
    *
    * @param spark
    * @param g
    */
  def writeAircraftVertices(spark: SparkSession, g: DseGraphFrame) = {

    val airCraftDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.0.2:5432/demo")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "bookings.aircrafts_data")
      .option("user", "postgres")
      .option("password", "example")
      .load()

    val airCraftV =
      airCraftDF
        .select(col("aircraft_code"), col("model"), col("range"))
        .withColumn("~label", lit("aircraft"))

    println("Writing aircraft vertices to flights Graph")

    g.updateVertices(airCraftV, Seq("aircraft"), false)

    println("completed writing aircraft vertices to flights Graph")

  }

  /** This method loads the airports vertices from the Postgres database into
    * the DSE Graph.
    *
    * @param spark
    * @param g
    */
  def writeAirportsVertices(spark: SparkSession, g: DseGraphFrame) = {

    val airportsDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.0.2:5432/demo")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "bookings.airports_data")
      .option("user", "postgres")
      .option("password", "example")
      .load()

    val airportsV = airportsDF
      .select(
        col("airport_code"),
        col("airport_name"),
        col("city"),
        col("coordinates"),
        col("timezone")
      )
      .withColumn("~label", lit("airport"))

    println("Writing airports vertices to flights Graph")

    g.updateVertices(airportsV, Seq("airport"), false)

    println("completed writing airports vertices to flights Graph")
  }

  /** This method loads the flights vertices from the Postgres database into the
    * DSE Graph.
    *
    * @param spark
    * @param g
    */
  def writeFlightVertices(spark: SparkSession, g: DseGraphFrame) = {

    val flightsDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.0.2:5432/demo")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "bookings.flights")
      .option("user", "postgres")
      .option("password", "example")
      .load()

    val flightsV = flightsDF
      .select(
        col("flight_id"),
        col("flight_no"),
        col("scheduled_departure"),
        col("scheduled_arrival"),
        col("actual_departure"),
        col("actual_arrival"),
        col("status")
      )
      .withColumn("~label", lit("flight"))

    println("Writing flights vertices to flights Graph")

    g.updateVertices(flightsV, Seq("flight"), false)

    println("completed writing flights vertices to flights Graph")

  }

  /** This method loads the tickets vertices from the Postgres database into the
    * DSE Graph.
    *
    * @param spark
    * @param g
    */
  def writeTicketVertices(spark: SparkSession, g: DseGraphFrame) = {

    val ticketsDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.0.2:5432/demo")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "bookings.tickets")
      .option("user", "postgres")
      .option("password", "example")
      .load()

    val ticketsV = ticketsDF
      .select(
        col("ticket_no"),
        col("book_ref"),
        col("passenger_id"),
        col("passenger_name"),
        col("contact_data")
      )
      .withColumn("~label", lit("ticket"))

    println("Writing tickets vertices to flights Graph")

    g.updateVertices(ticketsV, Seq("ticket"), false)

    println("completed writing tickets vertices to flights Graph")

  }

  // Used to test local host spark jobs
  def testLocalGraph(spark: SparkSession) = {
    val airportsDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/demo")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "bookings.airports_data")
      .option("user", "postgres")
      .option("password", "example")
      .load()

    airportsDF.printSchema()
    airportsDF.show()
  }
}
