package edu.harvard.schema

import java.time.LocalDateTime
import java.awt.Point

object GraphSchema {
  case class FlightV(
      flightId: String,
      flightNumber: String,
      scheduledDeparture: LocalDateTime,
      scheduledArrival: LocalDateTime,
      status: String,
      actualDeparture: LocalDateTime,
      actualArrival: LocalDateTime
  )
  case class TicketV(
      ticketNumber: String,
      book_date: LocalDateTime,
      passengerId: String,
      passengerName: String,
      contactData: String,
      fareConditions: String,
      amount: Double
  )
  case class AirportV(
      airportCode: String,
      airportName: String,
      city: String,
      coordinates: Point,
      timezone: String
  )
  case class AircraftV(
      aircraftCode: String,
      model: String,
      range: Int
  )
  case class OperatesE(
      seatNumber: String,
      conditions: String
  )
  case class BoardsE(
      boardingNumber: Int,
      seatNumber: String
  )
  case class RoutesE(
      arrivalAirportCode: String,
      departureAirportCode: String
  )
}
