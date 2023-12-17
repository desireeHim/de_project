PS. Kuna arhiivifail on liiga suur, et giti laadida, siis tuleks see ise igalühel omas arvutis siia projekti juurde lisada (zip fail lahti pakkida ja peaks olema jsoni kujul), see peaks olema alamkaustas 'project/data'.

Use `docker compose up -d` to start up the necessary services.  
    To stop services:  
    `docker compose stop`

    To stop and remove containers, if you add '-v' then also removes volumes:  
    `docker compose down`

    PS. remove volumes if you want to start totally from scratch, meaning all the database is also wiped clean, simple compose down only removes containers and not the volumes with data

Once the services are up and running (can take a minute), you can go to `localhost:8080` to access the Airflow UI.

The login information can be found in the compose yml (username and password: "airflow").

Move the `ingestion.py` file from the current folder to `/dags` folder (if not already there). In a few moments, it should appear in the Airflow UI.

For SQL create in dags folder subfolder 'sql' and move a copy of sql there.

This DAG has a connection to a Postgres database. Thus, before we can successfully execute the DAG, we need to set up the connection. This can be done in the Airflow UI.

### Airflow Postgres connection

From the top menu, navigate to Admin -> Connections.

Add a new record (+ sign).

Name the connection_id `airflow_pg`. This will be the reference used by the DAG.

For connection type, choose `Postgres`.

Host / schema / login / password can be found from the compose file.
host=postgres   
schema, login, password=airflow

You can test the connection. If it works, click on Save.

You also have to add connection for file system for File Sensor DAG.

Connection name is "data_path" and type is File (path)

### Airflow Neo4j connection
Likewise, add a Neo4j connection of type `Neo4j`

Name the connection_id `airflow_neo4j`.
- Host: neo4j
- port: 7687
- login: neo4j
- password: admin

### PgAdmin

You can access PgAdmin at `localhost:5050`, username: admin@admin.com and pw: root

To access database add new server:

name: whatever ex. "project"
host: postgres
port: leave the default value
username: airflow
pw: airflow

you should find the created and populated tables under project -> Databases -> airflow -> schemas -> tables
