package edu.harvard

import edu.harvard.schema._
import com.datastax.oss.driver.api.core.CqlSession
import java.net.InetSocketAddress
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.dse.driver.api.core.config.DseDriverOption
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import com.datastax.dse.driver.api.core.graph.DseGraph
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import java.time.Duration

object PersonApp extends App {

  val loader: DriverConfigLoader = DriverConfigLoader
    .programmaticBuilder()
    .withString(DseDriverOption.GRAPH_NAME, "demo")
    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(500))
    .build()

  val session = CqlSession
    .builder()
    .addContactPoint(new InetSocketAddress("localhost", 9042))
    .withLocalDatacenter("DC1")
    .withConfigLoader(loader)
    .build()

  val g: GraphTraversalSource = AnonymousTraversalSource
    .traversal()
    .withRemote(DseGraph.remoteConnectionBuilder(session).build())

  val personExample = new PersonExample(session, g)

  val john = Person("John", 30, "john@example.com")

  val person = personExample.addPersonVertex(john)
  val people = personExample.getPersons()

  personExample.addKnowsEdge("John", "Jane", 2020)
  personExample.queryKnownPeople("John")

  session.close()
}
