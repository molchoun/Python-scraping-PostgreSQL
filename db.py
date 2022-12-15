import psycopg2
from config import config
import scrape


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        create_table_category = ('''CREATE TABLE IF NOT EXISTS property_type 
                                      (id SERIAL PRIMARY KEY, 
                                      title VARCHAR(255), 
                                      q_string VARCHAR(16) UNIQUE)''',
                                 '''CREATE TABLE IF NOT EXISTS regions
                                      (id SERIAL PRIMARY KEY,
                                      name VARCHAR(255),
                                      q_string VARCHAR(16) UNIQUE''')
        cur.execute(create_table_category)
        values = scrape.get_categories_paths()
        insert_to_table = '''INSERT INTO property_type (title, q_string) VALUES (%s, %s)'''
        for key in values:
            cur.execute(insert_to_table, (key, values[key]))
        conn.commit()
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error in transaction, reverting all changes using rollback, error is: ', error)
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()
