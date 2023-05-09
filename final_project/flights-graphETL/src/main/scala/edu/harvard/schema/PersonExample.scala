package edu.harvard.schema

import com.datastax.dse.driver.api.core.graph.DseGraph
import com.datastax.oss.driver.api.core.CqlSession
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource

import java.net.InetSocketAddress

import collection.JavaConverters._
import java.util.UUID

case class Person(name: String, age: Int, email: String)

class PersonExample(session: CqlSession, g: GraphTraversalSource) {

  def addPersonVertex(person: Person): Unit = {
    g.addV("person")
      .property("person_id", UUID.randomUUID())
      .property("name", person.name)
      .property("age", person.age)
      .property("email", person.email)
      .as("person")
  }

  def getPersons(): Unit = {
    val persons = g.V().hasLabel("person").toList().asScala
    persons.foreach { person =>
      println(s"Person: ${person.property("name").value()}")
    }
  }

  def addKnowsEdge(
      person1Name: String,
      person2Name: String,
      since: Int
  ): Unit = {
    g.V()
      .hasLabel("person")
      .has("name", person1Name)
      .as("person1")
      .V()
      .hasLabel("person")
      .has("name", person2Name)
      .addE("knows")
      .from("person1")
      .property("since", since)
  }

  def queryKnownPeople(personName: String): Unit = {
    g.V()
      .hasLabel("person")
      .has("name", personName)
      .outE("knows")
      .has("since", P.gte(2020))
      .inV()
      .values("name")
      .asScala
      .foreach(println)
  }

}
