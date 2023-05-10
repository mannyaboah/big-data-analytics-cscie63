package edu.harvard.jobs

import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** This class loads the Edges from the Postgres database into the DSE Graph The
  * edges are loaded into the DSE Graph using the updateEdges method.
  */
object FlightGraphLoaderE extends App {
  val GRAPH_NAME = "flights"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("FlightGraphLoaderV")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  val g = spark.dseGraph(GRAPH_NAME)

  writeOperatesEdges(spark, g)
  writeBoardsEdges(spark, g)
  writeRoutesEdges(spark, g)

  /** This method loads the --[operates]--> edges from the Postgres database
    * into the DSE Graph.
    *
    * @param spark
    * @param g
    */
  def writeOperatesEdges(spark: SparkSession, g: DseGraphFrame) = {

    val opEdgesDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.0.2:5432/demo")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "bookings.operates")
      .option("user", "postgres")
      .option("password", "example")
      .load()

    val opEdges = opEdgesDF
      .withColumn("srcLabel", lit("flight"))
      .withColumn("dstLabel", lit("aircraft"))
      .withColumn("edgeLabel", lit("operates"))

    val flightToAircraft = opEdges
      .select(
        g.idColumn(col("srcLabel"), col("flight_no")) as "src",
        g.idColumn(col("dstLabel"), col("aircraft_code")) as "dst",
        col("edgeLabel") as "~label",
        col("seat_no"),
        col("fare_conditions")
      )

    flightToAircraft.show(5)

    println("Writing operates edges to flights Graph")

    g.updateEdges(flightToAircraft, false)

    println("completed writing operates edges to flights Graph")
  }

  /** This method loads the --[boards]--> edges from the Postgres database into
    * the DSE Graph.
    *
    * @param spark
    * @param g
    */
  def writeRoutesEdges(spark: SparkSession, g: DseGraphFrame) = {

    val query =
      "select flight_no, departure_airport, arrival_airport from bookings.routes"

    val routesEdgesDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.0.2:5432/demo")
      .option("driver", "org.postgresql.Driver")
      .option("query", query)
      .option("user", "postgres")
      .option("password", "example")
      .load()

    val routesEdges = routesEdgesDF
      .withColumn("srcLabel", lit("flight"))
      .withColumn("dstLabel", lit("airport"))
      .withColumn("edgeLabel", lit("routes"))

    val flightToAirport = routesEdges
      .select(
        g.idColumn(col("srcLabel"), col("flight_no")) as "src",
        g.idColumn(col("dstLabel"), col("arrival_airport")) as "dst",
        col("edgeLabel") as "~label",
        col("departure_airport"),
        col("arrival_airport")
      )

    println("writing routes edges to flights Graph")

    flightToAirport.show(5)

    g.updateEdges(flightToAirport, false)

    println("completed writing routes edges to flights Graph")
  }

  /** This method loads the --[boards]--> edges from the Postgres database into
    * the DSE Graph.
    *
    * @param spark
    * @param g
    */
  def writeBoardsEdges(spark: SparkSession, g: DseGraphFrame) = {

    val query =
      "select boarding_no, flight_id, seat_no, ticket_no from bookings.boarding_passes"

    val boardsEdgesDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.0.2:5432/demo")
      .option("driver", "org.postgresql.Driver")
      .option("query", query)
      .option("user", "postgres")
      .option("password", "example")
      .load()

    val boardsEdges = boardsEdgesDF
      .withColumn("srcLabel", lit("ticket"))
      .withColumn("dstLabel", lit("flight"))
      .withColumn("edgeLabel", lit("boards"))

    val ticketToFlight = boardsEdges
      .select(
        g.idColumn(col("srcLabel"), col("ticket_no")) as "src",
        g.idColumn(col("dstLabel"), col("flight_id")) as "dst",
        col("edgeLabel") as "~label",
        col("ticket_no"),
        col("boarding_no"),
        col("seat_no")
      )

    ticketToFlight.show(5)

    println("writing boards edges to flights Graph")

    g.updateEdges(ticketToFlight, false)

    println("completed writing boards edges to flights Graph")
  }
}
