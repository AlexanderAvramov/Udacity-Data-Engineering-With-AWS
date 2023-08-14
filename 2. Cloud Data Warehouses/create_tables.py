import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops a set of given tables. It needs a cursor object and a connection to run.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates a set of given tables. It needs a cursor object and a connection to run.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This reads the dwh.cfg file, connects as needed, and executes the drop_tables and create_tables functions described above.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()