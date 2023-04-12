CREATE (JohnWick3:Movie {title:'John Wick 3', released:2019, tagline:'Parabellum'})
CREATE (Chad:Person {name:'Chad Stahelski', born:1968})
CREATE
(Chad)-[:DIRECTED]->(JohnWick3);
MATCH (Keanu:Person {name: "Keanu Reeves"})
MATCH (Laurence:Person {name: "Laurence Fishburne"})
MATCH (HalleB:Person {name: "Halle Berry"})
MATCH (JohnWick3:Movie {title: "John Wick 3"})
CREATE
(Keanu)-[:ACTED_IN {roles:['John Wick']}]->(JohnWick3),
(Laurence)-[:ACTED_IN {roles:['Bowery King']}]->(JohnWick3),
(HalleB)-[:ACTED_IN {roles:['sofia']}]->(JohnWick3);