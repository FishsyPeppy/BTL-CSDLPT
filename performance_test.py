#!/usr/bin/env python3
"""
Performance test for partitioning functions
"""
import time
import psycopg2
import traceback
from Interface import *

def create_sample_data(num_records=1000):
    """Create a sample data file for testing"""
    print(f"Creating sample data with {num_records} records...")
    
    with open('sample_ratings.dat', 'w') as f:
        for i in range(num_records):
            userid = i % 1000 + 1
            movieid = i % 500 + 1
            rating = (i % 10) * 0.5  # Ratings from 0.0 to 4.5
            timestamp = 838985046 + i
            f.write(f"{userid}::{movieid}::{rating}::{timestamp}\n")
    
    print(f"✓ Created sample_ratings.dat with {num_records} records")

def benchmark_function(func, *args, **kwargs):
    """Benchmark a function and return execution time"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return end_time - start_time, result

def test_performance():
    """Test performance of partitioning functions"""
    try:
        print("=== Performance Testing ===\n")
        
        # Create sample data
        create_sample_data(10000)  # Start with 10K records
        
        # Connect to database
        conn = getopenconnection(dbname='dds_assgn1')
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        # Clean up existing tables
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = [row[0] for row in cur.fetchall()]
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.close()
        
        print("1. Testing loadratings function...")
        load_time, _ = benchmark_function(loadratings, 'ratings', 'sample_ratings.dat', conn)
        print(f"   ✓ Load time: {load_time:.2f} seconds")
        
        # Verify data loaded
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ratings")
        count = cur.fetchone()[0]
        print(f"   ✓ Loaded {count} records")
        cur.close()
        
        print("\n2. Testing range partitioning...")
        for num_partitions in [2, 3, 5]:
            print(f"   Testing with {num_partitions} partitions...")
            partition_time, _ = benchmark_function(rangepartition, 'ratings', num_partitions, conn)
            print(f"   ✓ Range partition ({num_partitions}): {partition_time:.2f} seconds")
            
            # Verify partitions created
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'range_part%'")
            partition_count = cur.fetchone()[0]
            print(f"   ✓ Created {partition_count} range partitions")
            cur.close()
            
            # Clean up range partitions
            for i in range(num_partitions):
                cur = conn.cursor()
                cur.execute(f"DROP TABLE IF EXISTS range_part{i}")
                cur.close()
        
        print("\n3. Testing round robin partitioning...")
        for num_partitions in [2, 3, 5]:
            print(f"   Testing with {num_partitions} partitions...")
            partition_time, _ = benchmark_function(roundrobinpartition, 'ratings', num_partitions, conn)
            print(f"   ✓ Round robin partition ({num_partitions}): {partition_time:.2f} seconds")
            
            # Verify partitions created
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'rrobin_part%'")
            partition_count = cur.fetchone()[0]
            print(f"   ✓ Created {partition_count} round robin partitions")
            cur.close()
            
            # Clean up round robin partitions
            for i in range(num_partitions):
                cur = conn.cursor()
                cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i}")
                cur.close()
        
        print("\n4. Testing insert functions...")
        
        # Setup partitions for insert testing
        rangepartition('ratings', 5, conn)
        roundrobinpartition('ratings', 5, conn)
        
        # Test range insert
        print("   Testing range insert...")
        insert_time, _ = benchmark_function(rangeinsert, 'ratings', 9999, 9999, 3.5, conn)
        print(f"   ✓ Range insert time: {insert_time:.4f} seconds")
        
        # Test round robin insert
        print("   Testing round robin insert...")
        insert_time, _ = benchmark_function(roundrobininsert, 'ratings', 9998, 9998, 3.5, conn)
        print(f"   ✓ Round robin insert time: {insert_time:.4f} seconds")
        
        conn.close()
        print("\n🎉 Performance testing completed!")
        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        traceback.print_exc()

def test_with_large_data():
    """Test with larger dataset if ratings.dat exists"""
    try:
        import os
        if not os.path.exists('ratings.dat'):
            print("ratings.dat not found. Skipping large data test.")
            return
        
        # Get file size
        file_size = os.path.getsize('ratings.dat') / (1024 * 1024)  # MB
        print(f"\nFound ratings.dat ({file_size:.1f} MB)")
        
        if file_size > 100:  # If larger than 100MB
            print("File is large. Testing with first 100,000 lines...")
            
            # Create subset
            with open('ratings.dat', 'r') as infile:
                with open('ratings_subset.dat', 'w') as outfile:
                    for i, line in enumerate(infile):
                        if i >= 100000:
                            break
                        outfile.write(line)
            
            print("Testing with subset...")
            conn = getopenconnection(dbname='dds_assgn1')
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            
            # Clean tables
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS ratings CASCADE")
            for i in range(10):
                cur.execute(f"DROP TABLE IF EXISTS range_part{i} CASCADE")
                cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i} CASCADE")
            cur.close()
            
            load_time, _ = benchmark_function(loadratings, 'ratings', 'ratings_subset.dat', conn)
            print(f"✓ Load time (100K records): {load_time:.2f} seconds")
            
            partition_time, _ = benchmark_function(rangepartition, 'ratings', 5, conn)
            print(f"✓ Range partition time: {partition_time:.2f} seconds")
            
            conn.close()
        
    except Exception as e:
        print(f"Large data test failed: {e}")

if __name__ == "__main__":
    test_performance()
    test_with_large_data()
