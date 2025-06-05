#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    Optimized for large files.
    """
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    
    try:
        # Drop table if exists to avoid conflicts
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename} CASCADE")
        
        # Create table with temporary columns for parsing
        cur.execute(f"""
            CREATE TABLE {ratingstablename}(
                userid integer, 
                extra1 char, 
                movieid integer, 
                extra2 char, 
                rating float, 
                extra3 char, 
                timestamp bigint
            )
        """)
        
        # Load data from file using COPY for better performance
        print(f"Loading data from {ratingsfilepath}...")
        with open(ratingsfilepath, 'r') as f:
            cur.copy_from(f, ratingstablename, sep=':')
        
        # Remove unnecessary columns
        cur.execute(f"""
            ALTER TABLE {ratingstablename} 
            DROP COLUMN extra1, 
            DROP COLUMN extra2, 
            DROP COLUMN extra3, 
            DROP COLUMN timestamp
        """)
        
        # Add indexes for better performance
        cur.execute(f"CREATE INDEX idx_{ratingstablename}_rating ON {ratingstablename}(rating)")
        cur.execute(f"CREATE INDEX idx_{ratingstablename}_userid ON {ratingstablename}(userid)")
        cur.execute(f"CREATE INDEX idx_{ratingstablename}_movieid ON {ratingstablename}(movieid)")
        
        # Analyze table for better query planning
        cur.execute(f"ANALYZE {ratingstablename}")
        
        con.commit()
        print("Data loading completed successfully!")
        
    except Exception as e:
        con.rollback()
        print(f"Error loading data: {e}")
        raise
    finally:
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    Optimized for large datasets.
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Create metadata table if not exists
        create_metadata_table(openconnection)
        
        # Drop existing range partitions if any
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'range_part%'")
        existing_tables = [row[0] for row in cur.fetchall()]
        for table in existing_tables:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        
        delta = 5.0 / numberofpartitions
        RANGE_TABLE_PREFIX = 'range_part'
        
        print(f"Creating {numberofpartitions} range partitions...")
        
        for i in range(numberofpartitions):
            minRange = i * delta
            maxRange = minRange + delta
            table_name = RANGE_TABLE_PREFIX + str(i)
            
            print(f"Creating partition {i}: [{minRange:.2f}, {maxRange:.2f}]")
            
            # Create partition table
            cur.execute(f"CREATE TABLE {table_name} (userid integer, movieid integer, rating float)")
            
            # Insert data based on range with optimized query
            if i == 0:
                # First partition includes the minimum value (0)
                cur.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating) 
                    SELECT userid, movieid, rating 
                    FROM {ratingstablename} 
                    WHERE rating >= {minRange} AND rating <= {maxRange}
                """)
            else:
                # Other partitions exclude the minimum value to avoid duplicates
                cur.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating) 
                    SELECT userid, movieid, rating 
                    FROM {ratingstablename} 
                    WHERE rating > {minRange} AND rating <= {maxRange}
                """)
            
            # Add indexes for better performance
            cur.execute(f"CREATE INDEX idx_{table_name}_rating ON {table_name}(rating)")
            cur.execute(f"CREATE INDEX idx_{table_name}_userid ON {table_name}(userid)")
            
            # Analyze table for better query planning
            cur.execute(f"ANALYZE {table_name}")
            
            # Get count for verification
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            print(f"  ✓ {table_name}: {count:,} records")
        
        # Save metadata
        save_partition_metadata(openconnection, RANGE_TABLE_PREFIX, numberofpartitions, 'range')
        
        con.commit()
        print("Range partitioning completed!")
        
    except Exception as e:
        con.rollback()
        print(f"Error in range partitioning: {e}")
        raise
    finally:
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    Optimized for large datasets.
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Create metadata table if not exists
        create_metadata_table(openconnection)
        
        # Drop existing round robin partitions if any
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'rrobin_part%'")
        existing_tables = [row[0] for row in cur.fetchall()]
        for table in existing_tables:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        
        RROBIN_TABLE_PREFIX = 'rrobin_part'
        
        print(f"Creating {numberofpartitions} round robin partitions...")
        
        for i in range(numberofpartitions):
            table_name = RROBIN_TABLE_PREFIX + str(i)
            
            # Create partition table
            cur.execute(f"CREATE TABLE {table_name} (userid integer, movieid integer, rating float)")
            
            # Insert data using round robin approach with optimized query
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating) 
                SELECT userid, movieid, rating 
                FROM (
                    SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rnum 
                    FROM {ratingstablename}
                ) as temp 
                WHERE MOD(temp.rnum-1, {numberofpartitions}) = {i}
            """)
            
            # Add indexes for better performance
            cur.execute(f"CREATE INDEX idx_{table_name}_userid ON {table_name}(userid)")
            cur.execute(f"CREATE INDEX idx_{table_name}_rating ON {table_name}(rating)")
            
            # Analyze table for better query planning
            cur.execute(f"ANALYZE {table_name}")
            
            # Get count for verification
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            print(f"  ✓ {table_name}: {count:,} records")
        
        # Save metadata
        save_partition_metadata(openconnection, RROBIN_TABLE_PREFIX, numberofpartitions, 'roundrobin')
        
        con.commit()
        print("Round robin partitioning completed!")
        
    except Exception as e:
        con.rollback()
        print(f"Error in round robin partitioning: {e}")
        raise
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Insert into main table
    cur.execute(f"INSERT INTO {ratingstablename}(userid, movieid, rating) VALUES ({userid}, {itemid}, {rating})")
    
    # Get total number of rows in main table
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    
    # Get number of partitions from metadata
    numberofpartitions, _ = get_partition_metadata(openconnection, RROBIN_TABLE_PREFIX)
    if numberofpartitions == 0:
        # Fallback to counting tables if metadata not found
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    
    # Calculate which partition to insert into
    index = (total_rows - 1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    
    # Insert into the specific partition
    cur.execute(f"INSERT INTO {table_name}(userid, movieid, rating) VALUES ({userid}, {itemid}, {rating})")
    
    cur.close()
    con.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Insert into main table
    cur.execute(f"INSERT INTO {ratingstablename}(userid, movieid, rating) VALUES ({userid}, {itemid}, {rating})")
    
    # Get number of partitions from metadata
    numberofpartitions, _ = get_partition_metadata(openconnection, RANGE_TABLE_PREFIX)
    if numberofpartitions == 0:
        # Fallback to counting tables if metadata not found
        numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    
    # Calculate partition index based on rating
    delta = 5.0 / numberofpartitions
    
    # Handle edge case for rating = 5.0 or ratings at partition boundaries
    if rating == 5.0:
        index = numberofpartitions - 1
    elif rating == 0.0:
        index = 0
    else:
        # For other values, we need to find which partition contains this rating
        # Partition i contains: (i*delta, (i+1)*delta] except partition 0 which contains [0, delta]
        index = int(rating / delta)
        if rating == index * delta and index > 0:
            # Rating is exactly at the boundary, belongs to previous partition
            index = index - 1
        
        # Ensure index is within bounds
        if index >= numberofpartitions:
            index = numberofpartitions - 1
    
    table_name = RANGE_TABLE_PREFIX + str(index)
    
    # Insert into the specific partition
    cur.execute(f"INSERT INTO {table_name}(userid, movieid, rating) VALUES ({userid}, {itemid}, {rating})")
    
    cur.close()
    con.commit()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def create_metadata_table(openconnection):
    """
    Create metadata table to store partition information
    """
    con = openconnection
    cur = con.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS partition_metadata (
                table_prefix VARCHAR(50),
                num_partitions INTEGER,
                partition_type VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()
    except Exception as e:
        print(f"Error creating metadata table: {e}")
    finally:
        cur.close()

def save_partition_metadata(openconnection, table_prefix, num_partitions, partition_type):
    """
    Save partition metadata
    """
    con = openconnection
    cur = con.cursor()
    try:
        # Delete existing metadata for this prefix
        cur.execute(f"DELETE FROM partition_metadata WHERE table_prefix = '{table_prefix}'")
        # Insert new metadata
        cur.execute(f"""
            INSERT INTO partition_metadata (table_prefix, num_partitions, partition_type) 
            VALUES ('{table_prefix}', {num_partitions}, '{partition_type}')
        """)
        con.commit()
    except Exception as e:
        print(f"Error saving partition metadata: {e}")
    finally:
        cur.close()

def get_partition_metadata(openconnection, table_prefix):
    """
    Get partition metadata
    """
    con = openconnection
    cur = con.cursor()
    try:
        cur.execute(f"SELECT num_partitions, partition_type FROM partition_metadata WHERE table_prefix = '{table_prefix}'")
        result = cur.fetchone()
        return result if result else (0, None)
    except Exception as e:
        print(f"Error getting partition metadata: {e}")
        return (0, None)
    finally:
        cur.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '{prefix}%'")
    count = cur.fetchone()[0]
    cur.close()
    return count
