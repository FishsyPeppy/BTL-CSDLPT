#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
from io import StringIO

# DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    con = openconnection
    cur = con.cursor()
    
    # Drop table if exists and create new one
    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename}")
    cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            userid integer,
            movieid integer,
            rating float
        )
    """)
    
    # Read and process file in chunks
    chunk_size = 100000
    with open(ratingsfilepath, 'r') as f:
        while True:
            chunk = []
            for _ in range(chunk_size):
                line = f.readline()
                if not line:
                    break
                parts = line.strip().split('::')  # File format uses :: as delimiter
                if len(parts) >= 3:  # userID::movieID::rating::timestamp
                    userid, movieid, rating = parts[0], parts[1], parts[2]
                    chunk.append(f"{userid}\t{movieid}\t{rating}\n")  # Use tab delimiter
            
            if not chunk:
                break
                
            # Create a string buffer for the chunk and use COPY
            buffer = StringIO(''.join(chunk))
            cur.copy_from(buffer, ratingstablename, sep='\t', columns=('userid', 'movieid', 'rating'))
            con.commit()
    
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions
    
    # Create all partition tables at once
    create_tables_sql = '; '.join([
        f"CREATE TABLE IF NOT EXISTS range_part{i} (userid integer, movieid integer, rating float)"
        for i in range(numberofpartitions)
    ])
    cur.execute(create_tables_sql)
    
    # Clear existing data in partitions
    cur.execute('; '.join([f"TRUNCATE TABLE range_part{i}" for i in range(numberofpartitions)]))
    
    # Insert data into partitions
    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        if i == 0:
            cur.execute("""
                INSERT INTO range_part{}
                SELECT userid, movieid, rating 
                FROM {}
                WHERE rating >= {} AND rating <= {}
            """.format(i, ratingstablename, minRange, maxRange))
        else:
            cur.execute("""
                INSERT INTO range_part{}
                SELECT userid, movieid, rating 
                FROM {}
                WHERE rating > {} AND rating <= {}
            """.format(i, ratingstablename, minRange, maxRange))
    
    cur.close()
    con.commit()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    
    # Create all partition tables at once
    create_tables_sql = '; '.join([
        f"CREATE TABLE IF NOT EXISTS rrobin_part{i} (userid integer, movieid integer, rating float)"
        for i in range(numberofpartitions)
    ])
    cur.execute(create_tables_sql)
    
    # Clear existing data in partitions
    cur.execute('; '.join([f"TRUNCATE TABLE rrobin_part{i}" for i in range(numberofpartitions)]))
    
    # Insert data into each partition
    for i in range(numberofpartitions):
        cur.execute("""
            INSERT INTO rrobin_part{}
            SELECT userid, movieid, rating
            FROM (
                SELECT *, ROW_NUMBER() OVER () - 1 as row_num
                FROM {}
            ) numbered_rows
            WHERE row_num % {} = {}
        """.format(i, ratingstablename, numberofpartitions, i))
    
    cur.close()
    con.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Insert into main table
        cur.execute("""
            INSERT INTO {} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """.format(ratingstablename), (userid, itemid, rating))
        
        # Get total count
        cur.execute("SELECT COUNT(*) FROM {}".format(ratingstablename))
        total_rows = cur.fetchone()[0]
        
        # Calculate partition index
        numberofpartitions = count_partitions('rrobin_part', openconnection)
        index = (total_rows - 1) % numberofpartitions
        
        # Insert into partition
        cur.execute("""
            INSERT INTO rrobin_part{} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """.format(index), (userid, itemid, rating))
        
        con.commit()
    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    
    # Get number of partitions
    numberofpartitions = count_partitions('range_part', openconnection)
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index -= 1
    
    # Insert into both tables in one transaction
    cur.execute("""
        BEGIN;
        INSERT INTO {} (userid, movieid, rating)
        VALUES (%s, %s, %s);
        INSERT INTO range_part{} (userid, movieid, rating)
        VALUES (%s, %s, %s);
        COMMIT;
    """.format(ratingstablename, index), 
    (userid, itemid, rating, userid, itemid, rating))
    
    cur.close()
    con.commit()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s', (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        print('A database named {0} already exists'.format(dbname))
    
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("""
        SELECT COUNT(*) 
        FROM pg_stat_user_tables 
        WHERE relname LIKE %s
    """, (prefix + '%',))
    count = cur.fetchone()[0]
    cur.close()
    return count
