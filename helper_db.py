import psycopg2
import psycopg2.extras
from config_db import config_db

table_queries = {
    'component_production': ["""
        DROP TABLE IF EXISTS public.component_production CASCADE;
        """,
                             """
                             CREATE TABLE public.component_production(
                                 id SERIAL PRIMARY KEY,
                                 component_id VARCHAR(100),
                                 date timestamp NOT NULL,
                                 value integer,
                                 unit VARCHAR(10),
                                 created_on TIMESTAMP default NOW()
                             );
                             """],
    'component_details': ["""
        DROP TABLE IF EXISTS public.component_details CASCADE;
        """,
                          """
                          CREATE TABLE public.component_details(
                              id SERIAL PRIMARY KEY,
                              component_id VARCHAR(100),
                              manufacturers_component_id VARCHAR,
                              type VARCHAR,
                              sub_type VARCHAR,
                              site_id VARCHAR,
                              data_provider VARCHAR,
                              manufacturer VARCHAR,
                              is_energy_producing BOOLEAN, 
                              created_on TIMESTAMP default NOW()
                          );
                          """],
    'weather': [""" DROP TABLE IF EXISTS public.weather;
        """,
                """
                CREATE TABLE weather
   (
       id SERIAL PRIMARY KEY,
       site_id VARCHAR(30),
       date TIMESTAMP NOT NULL,
       temperature_ambient numeric,
       temperature_module numeric,
       irradiance numeric,
       wind_direction numeric,
       wind_speed numeric
   );
                """],
    'production': ["""
        DROP TABLE IF EXISTS public.production CASCADE;
        """,
                   """
                   CREATE TABLE public.production (
                           site_id VARCHAR(30), 
                           measured_by VARCHAR(30),
                           date TIMESTAMP NOT NULL,
                           value integer, 
                           unit VARCHAR(10),
                           created_on TIMESTAMP default NOW()
                   );
               
                   """],
    'site': [
        """DROP TABLE IF EXISTS public.site CASCADE; 
        """,
        """
        CREATE TABLE public.site (
            site_id VARCHAR(20),
            name VARCHAR(100),
            account_id VARCHAR(100),
            status VARCHAR(20),
            size numeric,
            installation_date TIMESTAMP,
            pto_date TIMESTAMP,
            address VARCHAR(200),
            city VARCHAR(200),
            state CHAR(2),
            zip VARCHAR(12),
            timezone VARCHAR(50),
            latitude numeric,
            longitude numeric,
            owner_id VARCHAR(20),
            fetch_id VARCHAR(50),
            created_on TIMESTAMP default NOW()
   
            );
        """],
    'site_owner': [""" DROP TABLE IF EXISTS public.site_owner CASCADE; 
        """,
                   """
                   CREATE TABLE public.site_owner (
                       owner_id VARCHAR(20),
                       name VARCHAR(200),
                       status VARCHAR(20),
                       created_on TIMESTAMP default NOW()
           
                       );
                   """],
}


def create_tables(cloud_connect=True):
    queries = []
    for table in table_queries:
        queries += table_queries[table]
    run_queries(queries, cloud_connect=cloud_connect)
    return True


def get_db_params(cloud_connect=True):
    if cloud_connect:
        return config_db(filename='cloudsql.ini')
    else:
        return config_db(filename='database.ini')


def run_queries(commands, cloud_connect=True):
    conn = None
    try:
        params = get_db_params(cloud_connect)
        conn = psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        results = [] * len(commands)
        for command in commands:
            cur.execute(command)
            if cur.description:  # if 'description' is not None - there is something to fetch
                results.append(cur.fetchall())
            else:
                results.append(None)
        cur.close()
        conn.commit()
        return results
    except psycopg2.DatabaseError as error:
        print(error)
        return None
    finally:
        if conn:  # if not None
            conn.close()


def run_query(command, cloud_connect=True):
    result = run_queries([command], cloud_connect)
    return None if result is None else result[0]


def test_connection(cloud_connect=True):
    sql = ("""SELECT 1;""", """SELECT 2;""")
    return True if run_queries(sql, cloud_connect=cloud_connect) is not None else False


if __name__ == '__main__':
    create_tables()
