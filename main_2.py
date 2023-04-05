import pgmapmatch_execution
import  postgre_connection
import time

if __name__ == '__main__':

    print(f'Start: {time.ctime()}')

    # print('Preprocessing tables...')

    # postgre_connection.preprocess_tables() #Once tables created, comment the line

    # print('Obtaining routes to match...')

    routes = sorted(postgre_connection.obtain_routes()) #routes must be ordered to parallelize in different pythons --> select a sublist in each python

    # sublist = routes[:len(routes)//3]
    sublist = routes[len(routes)//3:2*len(routes)//3]
    # sublist = routes[2*len(routes)//3:]

    print('Initializing pgMapMatching...')

    mm = pgmapmatch_execution.initialize_pgmapmatching(verbose=False)

    print('Matching routes...')

    errors = pgmapmatch_execution.match_routes(sublist, mm)

    # print('Creating table with results...')

    # postgre_connection.create_pgmapmatch_results()

    with open('errors_2.txt','w') as tfile:
        tfile.write('\n'.join(errors))
        

    print(f'End: {time.ctime()}')