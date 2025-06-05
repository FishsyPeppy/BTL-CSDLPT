#!/usr/bin/env python3
"""
Full test with ratings.dat file
"""
import time
import os
from Interface import *

def test_full_ratings():
    """Test with full ratings.dat file"""
    
    if not os.path.exists('ratings.dat'):
        print("❌ ratings.dat not found!")
        return
    
    file_size = os.path.getsize('ratings.dat') / (1024 * 1024)  # MB
    print(f"📁 ratings.dat size: {file_size:.1f} MB")
    
    try:
        print("\n=== Testing with full ratings.dat ===")
        
        # Connect to database
        conn = getopenconnection(dbname='dds_assgn1')
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        # Clean up existing tables
        print("🧹 Cleaning up existing tables...")
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = [row[0] for row in cur.fetchall()]
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.close()
        
        # Load ratings
        print("📥 Loading ratings.dat...")
        start_time = time.time()
        loadratings('ratings', 'ratings.dat', conn)
        load_time = time.time() - start_time
        print(f"✓ Load completed in {load_time:.2f} seconds")
        
        # Check number of records
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ratings")
        total_records = cur.fetchone()[0]
        cur.close()
        print(f"✓ Loaded {total_records:,} records")
        
        # Test range partitioning
        print(f"\n📊 Testing range partitioning with {total_records:,} records...")
        for num_partitions in [2, 3, 5]:
            print(f"   Creating {num_partitions} range partitions...")
            start_time = time.time()
            rangepartition('ratings', num_partitions, conn)
            partition_time = time.time() - start_time
            print(f"   ✓ Range partition ({num_partitions}): {partition_time:.2f} seconds")
            
            # Verify partition counts
            cur = conn.cursor()
            total_in_partitions = 0
            for i in range(num_partitions):
                cur.execute(f"SELECT COUNT(*) FROM range_part{i}")
                count = cur.fetchone()[0]
                total_in_partitions += count
                print(f"     range_part{i}: {count:,} records")
            cur.close()
            
            if total_in_partitions == total_records:
                print(f"   ✓ All records correctly distributed")
            else:
                print(f"   ❌ Record count mismatch: {total_in_partitions} vs {total_records}")
            
            # Clean up range partitions
            cur = conn.cursor()
            for i in range(num_partitions):
                cur.execute(f"DROP TABLE IF EXISTS range_part{i}")
            cur.close()
            print()
        
        # Test round robin partitioning
        print(f"🔄 Testing round robin partitioning with {total_records:,} records...")
        for num_partitions in [2, 3, 5]:
            print(f"   Creating {num_partitions} round robin partitions...")
            start_time = time.time()
            roundrobinpartition('ratings', num_partitions, conn)
            partition_time = time.time() - start_time
            print(f"   ✓ Round robin partition ({num_partitions}): {partition_time:.2f} seconds")
            
            # Verify partition counts
            cur = conn.cursor()
            total_in_partitions = 0
            for i in range(num_partitions):
                cur.execute(f"SELECT COUNT(*) FROM rrobin_part{i}")
                count = cur.fetchone()[0]
                total_in_partitions += count
                print(f"     rrobin_part{i}: {count:,} records")
            cur.close()
            
            if total_in_partitions == total_records:
                print(f"   ✓ All records correctly distributed")
            else:
                print(f"   ❌ Record count mismatch: {total_in_partitions} vs {total_records}")
            
            # Clean up round robin partitions
            cur = conn.cursor()
            for i in range(num_partitions):
                cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i}")
            cur.close()
            print()
        
        # Test insert functions
        print("🔧 Testing insert functions...")
        
        # Setup partitions for insert testing
        rangepartition('ratings', 5, conn)
        roundrobinpartition('ratings', 5, conn)
        
        # Test range insert
        print("   Testing range insert...")
        start_time = time.time()
        rangeinsert('ratings', 999999, 999999, 4.5, conn)
        insert_time = time.time() - start_time
        print(f"   ✓ Range insert time: {insert_time:.4f} seconds")
        
        # Test round robin insert
        print("   Testing round robin insert...")
        start_time = time.time()
        roundrobininsert('ratings', 999998, 999998, 4.5, conn)
        insert_time = time.time() - start_time
        print(f"   ✓ Round robin insert time: {insert_time:.4f} seconds")
        
        conn.close()
        print("\n🎉 Full testing completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_full_ratings()
