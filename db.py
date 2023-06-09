import psycopg2
from config import config
import scrape
from psycopg2 import sql

class DB:

    def __init__(self):
        self.conn, self.cur = self.connect()
        self.conn.autocommit = True

    def table_exists(self, table_name):
        self.cur.execute("select exists"
                         "(select table_name "
                         "from information_schema.tables "
                         "where table_name=%s)", (table_name,))
        _ = self.cur.fetchone()[0]

        return _

    def create_table(self, name, columns):
        # name = "table_name"
        # columns = (("col1", "TEXT"), ("col2", "INTEGER"), ...)
        fields = []
        for col in columns:
            fields.append(sql.SQL("{} {}").format(sql.Identifier(col[0]), sql.SQL(col[1])))

        query = sql.SQL("CREATE TABLE {tbl_name} ( {fields} );").format(
            tbl_name=sql.Identifier(name),
            fields=sql.SQL(', ').join(fields)
        )
        self.cur.execute(query)
        self.conn.commit()

    @staticmethod
    def connect(conn=None):
        """ Connect to the PostgreSQL database server """

        try:
            params = config()
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            # print('Error in transaction, reverting all changes using rollback, error is: ', error)
            print('Error while connecting to database: ', error)

        return conn, cur

    def create_table_categories(self):
        create_table = ('''
                        CREATE TABLE IF NOT EXISTS property_type
                          (id SERIAL PRIMARY KEY,
                          name VARCHAR(255),
                          q_string VARCHAR(16) UNIQUE);
                        ''')
        self.cur.execute(create_table)
        insert_into_table = '''
                             INSERT INTO property_type (name, q_string)
                             VALUES (%s, %s)
                             ON CONFLICT (q_string) DO NOTHING;
                            '''
        categories_dict = scrape.get_categories()
        for key, value in categories_dict.items():
            self.cur.execute(insert_into_table, (key, value))
        self.conn.commit()

    def create_table_regions(self):
        create_table = ('''
                        CREATE TABLE IF NOT EXISTS property_type
                          (id SERIAL PRIMARY KEY,
                          name VARCHAR(255),
                          q_string VARCHAR(16) UNIQUE);
                        ''')
        self.cur.execute(create_table)
        insert_into_table = '''
                          INSERT INTO regions (name, q_string)
                          VALUES (%s, %s)
                          ON CONFLICT (q_string) DO NOTHING;
                          '''
        regions_dict = scrape.get_regions()
        for key, value in regions_dict.items():
            self.cur.execute(insert_into_table, (key, value))
        self.conn.commit()

    def create_table_urls(self):
        create_table = ('''
                        CREATE TABLE IF NOT EXISTS urls
                          (id SERIAL PRIMARY KEY,
                          url VARCHAR(255) UNIQUE,
                          category_id INT, 
                          region_id INT,
                          retrieved INTEGER DEFAULT 0,
                          CONSTRAINT fk_category 
                          FOREIGN KEY(category_id)
                          REFERENCES property_type(id),
                          CONSTRAINT fk_region
                          FOREIGN KEY(region_id)
                          REFERENCES regions(id));
                        ''')
        self.cur.execute(create_table)
        self.conn.commit()

    def create_tables(self):
        """ Create the tables and define relationships"""
        commands = (
            '''
                    CREATE TABLE IF NOT EXISTS garage_and_parking_sale
                        (id SERIAL PRIMARY KEY,
                        type VARCHAR(32),
                        floor_area SMALLINT,
                        address VARCHAR(255),
                        price NUMERIC,
                        currency CHAR(3),
                        utilities VARCHAR(255),
                        amenities VARCHAR(255),
                        link VARCHAR(255) UNIQUE,
                        date DATE)
                    ''',

            '''
                    CREATE TABLE IF NOT EXISTS garage_and_parking_rent
                        (id SERIAL PRIMARY KEY,
                        type VARCHAR(32),
                        floor_area SMALLINT,
                        address VARCHAR(255),
                        price NUMERIC,
                        currency CHAR(3),
                        utilities VARCHAR(255),
                        amenities VARCHAR(255),
                        link VARCHAR(255) UNIQUE,
                        date DATE)
                    ''',

            '''
                    CREATE TABLE IF NOT EXISTS event_venue_rent
                        (id SERIAL PRIMARY KEY,
                        type VARCHAR(32),
                        floor_area SMALLINT,
                        address VARCHAR(255),
                        price NUMERIC,
                        currency CHAR(3),
                        facilities VARCHAR(255),
                        max_guests VARCHAR(255),
                        event_types VARCHAR(255),
                        equipment VARCHAR(255),
                        quiet_hours BOOLEAN,
                        pets_allowed BOOLEAN,
                        link VARCHAR(255) UNIQUE,
                        date DATE)
                    ''',

            '''
                    CREATE TABLE IF NOT EXISTS apartment_rent
                        (id SERIAL PRIMARY KEY,
                        const_type VARCHAR(10),
                        new_const BOOLEAN,
                        elevator BOOLEAN,
                        b_floors SMALLINT,
                        house_has VARCHAR(255),
                        floor_area SMALLINT,
                        rooms SMALLINT,
                        bathrooms SMALLINT,
                        ceil_height REAL,
                        floor SMALLINT,
                        balcony VARCHAR(32),
                        furniture VARCHAR(32),
                        renovation VARCHAR(32),
                        amenities VARCHAR(255),
                        appliances VARCHAR(255),
                        guests SMALLINT,
                        pets_allowed BOOLEAN,
                        child_allowed BOOLEAN,
                        utility_pay VARCHAR(16),
                        address VARCHAR(255),
                        price NUMERIC,
                        currency CHAR(3),
                        link VARCHAR(255) UNIQUE,
                        date DATE,
                        region_id REFERENCES regions(id))
                    ''',

            '''
                    CREATE TABLE IF NOT EXISTS cml_property_sale
                        (id SERIAL PRIMARY KEY,
                        type VARCHAR(32),
                        const_type VARCHAR(10),
                        floor_area SMALLINT,
                        address VARCHAR(255),
                        price NUMERIC,
                        currency CHAR(3),
                        furniture VARCHAR(32),
                        elevator BOOLEAN,
                        line VARCHAR(16),
                        parking VARCHAR(32),
                        link VARCHAR(255) UNIQUE,
                        date DATE)
                    ''',

            '''
                    CREATE TABLE IF NOT EXISTS cml_property_rent
                        (id SERIAL PRIMARY KEY,
                        type VARCHAR(32),
                        floor_area SMALLINT,
                        address VARCHAR(255),
                        price NUMERIC,
                        currency CHAR(3),
                        furniture VARCHAR(32),
                        elevator BOOLEAN,
                        line VARCHAR(16),
                        parking VARCHAR(32),
                        link VARCHAR(255) UNIQUE,
                        date DATE)
                    ''',

            '''
                    CREATE TABLE IF NOT EXISTS house_rent
                        (id SERIAL PRIMARY KEY,
                        const_type VARCHAR(10),
                        new_const BOOLEAN,
                        elevator BOOLEAN,
                        b_floors SMALLINT,
                        house_has VARCHAR(255),
                        floor_area SMALLINT,
                        rooms SMALLINT,
                        bathrooms SMALLINT,
                        ceil_height REAL,
                        floor SMALLINT,
                        balcony VARCHAR(32),
                        furniture VARCHAR(32),
                        renovation VARCHAR(32),
                        amenities VARCHAR(255),
                        appliances VARCHAR(255),
                        guests SMALLINT,
                        pets_allowed BOOLEAN,
                        child_allowed BOOLEAN,
                        utility_pay VARCHAR(16),
                        address VARCHAR(255),
                        price NUMERIC,
                        currency CHAR(3),
                        link VARCHAR(255) UNIQUE,
                        date DATE,
                        region_id REFERENCES regions(id))
                    ''',

        )
