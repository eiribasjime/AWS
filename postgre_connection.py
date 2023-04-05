import psycopg2
import pandas as pd


def connect_to_database():
    '''
    Conectarse con la base de datos PostgreSQL
    '''
    try:
        connection = psycopg2.connect(database='postgres', user='postgres', password='mn23hqiq', host='ec2-3-91-29-93.compute-1.amazonaws.com', port=5432)
        
        return connection
            
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def preprocess_tables():
    '''
    Una vez están cargadas las tablas, se preprocesan para poder trabajar con ellas
    '''

    connection = connect_to_database()

    try:

        cursor = connection.cursor()

        prep_query = ("ALTER TABLE streets_table_nav_lr_mod "
            "ALTER COLUMN geom_way TYPE geometry(linestring,32630) USING ST_GeometryN(geom_way, 1);")
        
        prep_query3 = ("CREATE TABLE if not exists puntos_gps ( " # CREATE TABLE if not exists
        "ID_R char(30), "
        "DATETIME timestamp, "	
        "ELE numeric, "
        "ID_SENSOR char(8), "
        "SEGMEN_ID int, "
        "SECUENCIA int, "
        "DIST numeric, "
        "SECS int, "
        "V_KMH numeric, "
        "VEHIC char(5), "
        "DIST_V char(5), "
        "ATRIBUCION char(6), "
        "DATE_R date, "
        "YEAR_R smallint, "
        "MONTH_R smallint, "
        "DAY_R smallint, "
        "HOUR_R smallint, "
        "HORAUTC smallint, "
        "LAT_COR double precision, "
        "LON_COR double precision"
        ");")

        prep_query4 = "ALTER TABLE puntos_gps ADD COLUMN geom geometry(PointM,4326);"

        prep_query5 = "ALTER TABLE puntos_gps ADD COLUMN datetime_unix FLOAT;"
        prep_query6 = "UPDATE puntos_GPS SET datetime_unix = EXTRACT(EPOCH FROM datetime);"

        prep_query7 = ("SELECT AddGeometryColumn ('public','puntos_gps','geom_unix',4326,'POINTM',3); "
            "UPDATE puntos_gps SET geom_unix = ST_SetSRID(ST_MakePointM(lon_cor,lat_cor,datetime_unix),4326);")

        prep_query8 = "ALTER TABLE puntos_gps ALTER COLUMN geom_unix TYPE Geometry(POINTM, 32630) USING ST_Transform(geom_unix, 32630);"

        prep_query9 = ("CREATE TABLE rutas_gps AS "
            "SELECT secuencia, ST_MakeLine(geom_unix ORDER BY datetime ASC) AS trace_geom "
            "FROM puntos_gps "
            "GROUP BY secuencia;")

        prep_query_10 = "ALTER TABLE rutas_gps ALTER COLUMN trace_geom TYPE Geometry(LineStringM, 32630) USING ST_Transform(trace_geom, 32630);"

        # cursor.execute(prep_query)
        # cursor.execute(prep_query3)
        cursor.execute(prep_query4)
        cursor.execute(prep_query5)
        cursor.execute(prep_query6)
        cursor.execute(prep_query7)
        cursor.execute(prep_query8)
        cursor.execute(prep_query9)
        cursor.execute(prep_query_10)

        connection.commit()

        connection.close()

    except Exception as e:
        
        connection.close()
        print(e)
        

def obtain_routes():
    '''
    Obtener las rutas a procesar
    '''

    connection = connect_to_database()

    try:

        cursor = connection.cursor()

        query = 'SELECT secuencia FROM "public"."rutas_gps";'

        cursor.execute(query)
        connection.commit()

        routes = cursor.fetchall()

        rutas = [a[0] for a in routes]

        connection.close()

        return rutas
    
    except Exception as e:
        
        connection.close()
        print(e)

def create_pgmapmatch_results():
    '''
    Crear la tabla SQL llamada resultados_pgmapmatch en la que se guardan los resultados del mapmatching
    '''

    connection = connect_to_database()

    try:

        cursor = connection.cursor()

        query2 = "DROP TABLE IF EXISTS resultados_pgmapmatch;"
    
        query3 = ("CREATE TABLE resultados_pgmapmatch AS "
    "SELECT coord.*, "
    "ST_DWithin(coord.geom_unix,rutas.matched_line, 100), "
    "ST_Distance(coord.geom_unix, rutas.matched_line) as min, "
    "ST_LineInterpolatepoint(rutas.matched_line,ST_LineLocatePoint(rutas.matched_line,coord.geom_unix)) as geom_proyectado, "
    "ST_AsText(ST_AsEWKT(ST_Transform(ST_LineInterpolatepoint(rutas.matched_line,ST_LineLocatePoint(rutas.matched_line,coord.geom_unix)), 4326))) as geom_proyectado_text "
    "FROM puntos_gps as coord "
    "JOIN rutas_gps as rutas "
    "ON coord.secuencia = rutas.secuencia " 
    "Order by coord.datetime_unix asc; ")
        
        query5 = ("ALTER TABLE resultados_pgmapmatch ALTER COLUMN geom_proyectado "
    "TYPE Geometry(Point, 4326) "
    "USING ST_Transform(geom_proyectado, 4326)")

        query8 = "ALTER TABLE resultados_pgmapmatch ADD COLUMN lon_sintetic text;"
        query9 = "ALTER TABLE resultados_pgmapmatch ADD COLUMN lat_sintetic text;"

        query10 = "UPDATE resultados_pgmapmatch SET lon_sintetic = SUBSTRING(SPLIT_PART(geom_proyectado_text, ' ', 1),7);"
        query11 = "UPDATE resultados_pgmapmatch SET lat_sintetic = SUBSTRING(SPLIT_PART(geom_proyectado_text, ' ', 2),0, length(SPLIT_PART(geom_proyectado_text, ' ', 2)));"
        
        cursor.execute(query2)
        cursor.execute(query3)
        cursor.execute(query5)

        cursor.execute(query8)
        cursor.execute(query9)
        cursor.execute(query10)
        cursor.execute(query11)

        connection.commit()

        connection.close()

    except Exception as e:
        
        connection.close()
        print(e)