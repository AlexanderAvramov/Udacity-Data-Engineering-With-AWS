import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, validate_tables_queries
from create_tables import drop_tables, create_tables


def load_staging_tables(cur, conn):
    """
    This function loads the data from S3 into the staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts the tables into the dimensional model.
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def validate_tables(cur, conn):
    """
    This function inserts the tables into the dimensional model.
    """
    for query in validate_tables_queries:
        cur.execute(query)
        data = cur.fetchall()
        print(query)
        print(data)


def main():
    """
    This is the main function that returns the connection, loads the staging tables, and inserts them into a
    dimensional model.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # drop the tables
    drop_tables(cur, conn)

    # create the tables
    create_tables(cur, conn)

    # load
    load_staging_tables(cur, conn)

    # insert
    insert_tables(cur, conn)

    # validate
    validate_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
