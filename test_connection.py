#!/usr/bin/env python3
"""
Test connection to PostgreSQL database
"""
import psycopg2
import traceback
from Interface import getopenconnection

def test_connection():
    """Test PostgreSQL connection"""
    try:
        print("Testing PostgreSQL connection...")
        
        # Test connection to default postgres database
        conn = getopenconnection()
        print("✓ Successfully connected to PostgreSQL!")
        
        # Test basic query
        cur = conn.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        print(f"✓ PostgreSQL version: {version}")
        
        # Test database creation
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur.execute("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='dds_assgn1'")
        count = cur.fetchone()[0]
        
        if count == 0:
            cur.execute("CREATE DATABASE dds_assgn1")
            print("✓ Created database 'dds_assgn1'")
        else:
            print("✓ Database 'dds_assgn1' already exists")
        
        cur.close()
        conn.close()
        
        # Test connection to assignment database
        conn = getopenconnection(dbname='dds_assgn1')
        print("✓ Successfully connected to dds_assgn1 database!")
        
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
        table_count = cur.fetchone()[0]
        print(f"✓ Found {table_count} tables in public schema")
        
        cur.close()
        conn.close()
        
        print("\n🎉 All connection tests passed!")
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure PostgreSQL is running")
        print("2. Check username/password (default: postgres/1234)")
        print("3. Verify PostgreSQL is listening on localhost:5432")
        print("4. Check PostgreSQL service status")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_connection()
