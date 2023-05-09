system.graph('demo').ifNotExists().create()

:remote config alias g demo.g

schema.vertexLabel('person').ifNotExists().partitionBy('person_id', Uuid).property('name', Text).property('gender', Text).property('age', Int).create()
schema.vertexLabel('master').ifNotExists().partitionBy('master_id', Uuid).property('passport_id', Text).create()
schema.vertexLabel('book').ifNotExists().partitionBy('book_id', Uuid).property('name', Text).property('author', Text).property('price', Double).create()
schema.edgeLabel('knows').ifNotExists().from('person').to('person').property('since', Date).create()
schema.edgeLabel('authored').ifNotExists().from('person').to('book').create()
schema.edgeLabel('is').ifNotExists().from('person').to('master').create()
