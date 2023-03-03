from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2


def make_database():
    """
    Make the Postgres database and create the table.
    """

    dbname = 'weatherdb'
    username = 'postgres'
    tablename = 'weather_table'
    host = 'localhost'
    port = 5432

    # Note: I didn't make a password.
    engine = create_engine('postgresql+psycopg2://%s@localhost/%s' % (username, dbname))

    if not database_exists(engine.url):
        create_database(engine.url)

    conn = psycopg2.connect(database=dbname, user=username, host=host, port=port)

    curr = conn.cursor()

    create_table = """CREATE TABLE IF NOT EXISTS %s
                     (
                     city TEXT, 
                     country TEXT,
                     latitude REAL,
                     longitude REAL,
                     humidity REAL,
                     pressure REAL,
                     temperature REAL,
                     weather TEXT,
                     date DATE
                     )
                     """ % tablename

    curr.execute(create_table)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    make_database()