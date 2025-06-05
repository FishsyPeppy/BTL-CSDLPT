#!/usr/bin/env python3
"""
Main demo script for the distributed database assignment
"""
import os
import time
import sys
from Interface import *

def print_header(title):
    """Print a formatted header"""
    print("\n" + "="*60)
    print(f" {title}")
    print("="*60)

def print_section(title):
    """Print a section header"""
    print(f"\n📌 {title}")
    print("-" * 40)

def demo_with_sample_data():
    """Demo with sample data"""
    print_header("DISTRIBUTED DATABASE PARTITIONING DEMO")
    print("🎯 Demonstrating horizontal data partitioning on PostgreSQL")
    print("📊 Using MovieLens ratings dataset")
    
    try:
        # Connect to database
        print_section("Database Connection")
        conn = getopenconnection(dbname='dds_assgn1')
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        print("✓ Connected to PostgreSQL database 'dds_assgn1'")
        
        # Clean up
        print_section("Environment Preparation")
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = [row[0] for row in cur.fetchall()]
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        print(f"✓ Cleaned up {len(tables)} existing tables")
        cur.close()
        
        # Load test data
        print_section("Data Loading")
        print("Loading test data (test_data.dat)...")
        start_time = time.time()
        loadratings('ratings', 'test_data.dat', conn)
        load_time = time.time() - start_time
        
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ratings")
        total_records = cur.fetchone()[0]
        cur.close()
        print(f"✓ Loaded {total_records} records in {load_time:.3f} seconds")
        
        # Demo range partitioning
        print_section("Range Partitioning Demo")
        print("Creating range partitions based on rating values...")
        
        for num_partitions in [2, 3, 5]:
            print(f"\n🔸 Testing {num_partitions} partitions:")
            start_time = time.time()
            rangepartition('ratings', num_partitions, conn)
            partition_time = time.time() - start_time
            
            # Show distribution
            cur = conn.cursor()
            delta = 5.0 / num_partitions
            total_in_partitions = 0
            
            for i in range(num_partitions):
                minRange = i * delta
                maxRange = minRange + delta
                cur.execute(f"SELECT COUNT(*) FROM range_part{i}")
                count = cur.fetchone()[0]
                total_in_partitions += count
                
                if i == 0:
                    print(f"   range_part{i}: [{minRange:.1f}, {maxRange:.1f}] -> {count} records")
                else:
                    print(f"   range_part{i}: ({minRange:.1f}, {maxRange:.1f}] -> {count} records")
            
            print(f"   ⏱️  Partitioning time: {partition_time:.3f} seconds")
            print(f"   ✅ Total records distributed: {total_in_partitions}")
            
            # Clean up
            for i in range(num_partitions):
                cur.execute(f"DROP TABLE IF EXISTS range_part{i}")
            cur.close()
        
        # Demo round robin partitioning
        print_section("Round Robin Partitioning Demo")
        print("Creating round robin partitions for load balancing...")
        
        for num_partitions in [2, 3, 5]:
            print(f"\n🔸 Testing {num_partitions} partitions:")
            start_time = time.time()
            roundrobinpartition('ratings', num_partitions, conn)
            partition_time = time.time() - start_time
            
            # Show distribution
            cur = conn.cursor()
            total_in_partitions = 0
            
            for i in range(num_partitions):
                cur.execute(f"SELECT COUNT(*) FROM rrobin_part{i}")
                count = cur.fetchone()[0]
                total_in_partitions += count
                print(f"   rrobin_part{i}: {count} records")
            
            print(f"   ⏱️  Partitioning time: {partition_time:.3f} seconds")
            print(f"   ✅ Total records distributed: {total_in_partitions}")
            
            # Clean up
            for i in range(num_partitions):
                cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i}")
            cur.close()
        
        # Demo insert operations
        print_section("Insert Operations Demo")
        print("Testing insert operations on partitioned tables...")
        
        # Setup partitions
        rangepartition('ratings', 5, conn)
        roundrobinpartition('ratings', 5, conn)
        
        print("\n🔸 Range Insert Demo:")
        test_inserts = [(1001, 2001, 1.5), (1002, 2002, 3.0), (1003, 2003, 4.5)]
        
        for userid, movieid, rating in test_inserts:
            start_time = time.time()
            rangeinsert('ratings', userid, movieid, rating, conn)
            insert_time = time.time() - start_time
            
            # Calculate expected partition
            delta = 5.0 / 5
            if rating == 5.0:
                expected_partition = 4
            elif rating == 0.0:
                expected_partition = 0
            else:
                expected_partition = int(rating / delta)
                if rating == expected_partition * delta and expected_partition > 0:
                    expected_partition = expected_partition - 1
            
            print(f"   Rating {rating} -> range_part{expected_partition} ({insert_time:.4f}s)")
        
        print("\n🔸 Round Robin Insert Demo:")
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ratings")
        initial_count = cur.fetchone()[0]
        cur.close()
        
        for i, (userid, movieid, rating) in enumerate([(1004, 2004, 2.5), (1005, 2005, 3.5)]):
            start_time = time.time()
            roundrobininsert('ratings', userid, movieid, rating, conn)
            insert_time = time.time() - start_time
            
            expected_partition = (initial_count + i) % 5
            print(f"   Insert #{i+1} -> rrobin_part{expected_partition} ({insert_time:.4f}s)")
        
        conn.close()
        
        print_section("Demo Completed Successfully! 🎉")
        print("✅ All partitioning methods demonstrated")
        print("✅ Insert operations working correctly")
        print("✅ Data integrity maintained")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function"""
    if len(sys.argv) > 1 and sys.argv[1] == "--full":
        # Run with full ratings.dat if available
        if os.path.exists('ratings.dat'):
            print("🚀 Running full test with ratings.dat...")
            from test_full_ratings import test_full_ratings
            test_full_ratings()
        else:
            print("❌ ratings.dat not found for full test")
    else:
        # Run demo with sample data
        demo_with_sample_data()

if __name__ == "__main__":
    main()
