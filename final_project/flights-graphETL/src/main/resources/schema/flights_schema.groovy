// Create database schema for the flights graph
system.graph('flights').ifNotExists().create()

// Set the traversal source to the flights graph
:remote config alias g flights.g

// vertex labels
schema.vertexLabel('airport').ifNotExists().partitionBy('airport_code', Text).property('airport_name', Text).property('city', Text).property('coordinates', Text).property('timezone', Text).create()
schema.vertexLabel('aircraft').ifNotExists().partitionBy('aircraft_code', Text).property('model', Text).property('range', Int).create()
schema.vertexLabel('flight').ifNotExists().partitionBy('flight_id', Text).property('flight_number', Text).property('scheduled_departure', Timestamp).property('scheduled_arrival', Timestamp).property('status', Text).property('actual_departure', Timestamp).property('actual_arrival', Timestamp).create()
schema.vertexLabel('ticket').ifNotExists().partitionBy('ticket_no', Text).property('book_date', Timestamp).property('passenger_id', Text).property('passenger_name', Text).property('contact_data', Text).property('fare_conditions', Text).property('amount', Double).create()

// Edge labels
schema.edgeLabel('routes').ifNotExists().from('flight').to('airport').property('arrival_airport', Text).property('departure_airport', Text).create()
schema.edgeLabel('boards').ifNotExists().from('ticket').to('flight').property('seat_no', Text).property('boarding_no', Int).create()
schema.edgeLabel('operates').ifNotExists().from('flight').to('aircraft').property('seat_no', Text).property('conditions', Text).create()