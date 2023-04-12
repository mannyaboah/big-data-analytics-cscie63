"""🐍 App that reads astronaut csv and loads it into cassadra table
"""

import csv
import logging

from cassandra.cluster import Cluster

# Log configurations
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


KEYSPACE = "hw07_p3"

def main():
    # Connect to dse cluster
    cluster = Cluster()
    session = cluster.connect()

    # If keyspace already exists, drop it
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")

    if KEYSPACE in [row[0] for row in rows]:
        log.info(f"dropping existing keyspace: {KEYSPACE}")
        session.execute("DROP KEYSPACE " + KEYSPACE)
    
    # Create the keyspace
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}
        """ % KEYSPACE
        )
    
    # Set the keyspace
    log.info(f"Setting keyspace to {KEYSPACE}")
    session.set_keyspace(keyspace=KEYSPACE)
    
    # Create cosmonauts table
    log.info("creating astronauts table...")
    session.execute("""
        CREATE TABLE astronauts (
            name text, 
            year int, 
            group int,
            status text,
            dob text,
            birthplace text,
            gender text,
            alma_mater text,
            spaceflights int,
            spaceflight_hours int,
            spacewalks int,
            spacewalk_hours int,
            missions text,
            PRIMARY KEY (group, name)
        )
        """)
    
    prepared_stmnt = session.prepare("""
                INSERT INTO astronauts (name,year,group,status,dob,birthplace,
                gender,alma_mater,spaceflights,spaceflight_hours,spacewalks,spacewalk_hours,missions)
                VALUES (?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)
    
    # Read csv file
    with open("astronauts.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                log.info(f'Writing columns: {", ".join(row)}')
                line_count +=1
            else:
                log.info(f"""about to write line: \n
                {row[0]}, {row[1]}, 
                {row[2]}, {row[3]}, {row[4]}, 
                {row[5]}, {row[6]}, {row[7]}, 
                {row[8]}, {row[9]}, {row[10]}, 
                {row[11]}, {row[12]}"""
                )

                name = row[0]
                year = int(row[1])
                group = int(row[2])
                status = row[3]
                dob = row[4]
                birthplace = row[5]
                gender = row[6]
                alma_mater = row[7]
                spaceflights = int(row[8])
                spaceflight_hours = int(row[9])
                spacewalks = int(row[10])
                spacewalk_hours = int(row[11])
                missions = row[12]

                session.execute(prepared_stmnt,
                                [name, 
                                 year, 
                                 group, 
                                 status, 
                                 dob, 
                                 birthplace, 
                                 gender, 
                                 alma_mater, 
                                 spaceflights, 
                                 spaceflight_hours, 
                                 spacewalks, 
                                 spacewalk_hours, 
                                 missions]
                                )
                log.info(f'line: {line_count} processed')
                line_count +=1

    log.info(f"Finished processing {line_count} lines of astronaut data.")

    csv_file.close()
    
    session.shutdown()

# Main method
if __name__ == "__main__":
    main()