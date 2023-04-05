import sys
import tqdm


sys.path.append(r'C:\Users\eiribasjime') #pgmapmatching route
import pgMapMatch as pgmatch

def initialize_pgmapmatching(streets_table='streets_table_nav_lr_mod', verbose=True):

    try:

        mm = pgmatch.mapMatcher(streets_table, traceTable='rutas_gps', idName='secuencia', geomName='trace_geom', verbose=verbose)

        return mm

    except Exception as e:

        print(e)

def match_routes(rutas, mm):

    errors = []

    for i in tqdm.tqdm(range(len(rutas))):

        try:

            mm.matchPostgresTrace(rutas[i])
            mm.writeGeomToPostgres()
        
        except Exception as e:

            errors.append(rutas[i])
            print(e)
            
    return errors