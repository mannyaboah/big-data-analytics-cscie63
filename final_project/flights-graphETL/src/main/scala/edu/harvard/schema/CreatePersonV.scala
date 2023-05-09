package edu.harvard.schema

import com.datastax.driver.core.DataType
import com.datastax.dse.driver.api.core.config.DseDriverOption
import com.datastax.dse.driver.api.core.graph.DseGraph
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource

import java.net.InetSocketAddress
import java.util.UUID

object CreatePersonV extends App {

  val loader: DriverConfigLoader = DriverConfigLoader
    .programmaticBuilder()
    .withString(DseDriverOption.GRAPH_NAME, "demo")
    .build()

  val session = CqlSession
    .builder()
    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
    .withLocalDatacenter("DC1")
    .withConfigLoader(loader)
    .build()

  val g: GraphTraversalSource = DseGraph.g
    .withRemote(DseGraph.remoteConnectionBuilder(session).build())

  val res = g
    .addV("person")
    .property("person_id", UUID.randomUUID())
    .property("name", "manny")
    .property("age", 30)

  println(res.label())

}
