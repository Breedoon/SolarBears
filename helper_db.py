#!/usr/bin/python

import psycopg2
from config_db import config_db

# -- id serial GENERATED ALWAYS AS IDENTITY,
# https://chartio.com/resources/tutorials/how-to-define-an-auto-increment-primary-key-in-postgresql/
# http://www.postgresqltutorial.com/postgresql-unique-constraint/
# http://www.postgresqltutorial.com/postgresql-identity-column/


table_queries = {'component_production': ["""
        DROP TABLE IF EXISTS public.component_production CASCADE;
        """,
                          """
                          CREATE TABLE public.component_production(
                              id SERIAL PRIMARY KEY,
                              component_id VARCHAR(100),
                              date timestamp NOT NULL,
                              value numeric,
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
             CREATE TABLE weather(
                 id serial,
                 site_id VARCHAR(30),
                 date TIMESTAMP,
                 weather_adj numeric
                 
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
         owner_id VARCHAR(20),
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
    """ create tables in the PostgreSQL database"""
    queries = []
    for table in table_queries:
        queries += table_queries[table]
    run_query(queries)
    # print('creating communication table: ', create_communication_interval_per_site_id(cloud_connect))
    return True


def get_db_params(cloud_connect=True):
    print('testing cloud connect = ', cloud_connect)

    if cloud_connect:

        print('Connection type: Cloud')

        # if str(self.company_name).lower() = 'barrier':
        #    return config(filename='barrier_cloudsql.ini')

        return config_db(filename='cloudsql.ini')

    else:

        print('Connection type: Localhost')

        return config_db(filename='database.ini')


def run_query(commands, cloud_connect=True):
    conn = None
    try:

        # read the connection parameters
        params = get_db_params(cloud_connect)
        print(params)
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        # success!
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn:
            conn.close()


def test_connection(cloud_connect=True):
    # add print params to the run_query method
    print('hello world')
    print('test query')
    sql = ("""SELECT 1;""", """SELECT 2;""")
    run_query(sql, cloud_connect=True)


if __name__ == '__main__':
    company_name = 'solectria_sandbox'
    # create_all_tables(cloud_connect=True, company_name=company_name)
    create_tables(cloud_connect=True)

'''
SELECT d.site_id, date_trunc('Day',p.date), sum(p.value)/1000
FROM component_production p
JOIN component_details d
ON p.component_id = d.manufacturers_component_id
WHERE d.site_id = '225542'
GROUP BY 1,2
ORDER BY 2
limit 100;

CREATE TABLE production_guarantee (
    contract_id serial,
    site_id varchar,
    contract_start_date DATE,
    contract_end_date DATE,
    production_guarantee numeric,
    unit varchar,
    term_months int
)
'''
"""
-- DROP TABLE IF EXISTS public.communication_interval_per_site_id;
"""
"""
CREATE TABLE public.communication_interval_per_site_id
(
id SERIAL PRIMARY KEY,
site_id character varying(30),
date timestamp without time zone,
comm_interval integer,
comm_interval_expected integer
);
"""
