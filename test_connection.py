#!/usr/bin/env python3
"""
Test connection to PostgreSQL database
"""
import psycopg2
from Interface import getopenconnection

def test_connection():
    """Test PostgreSQL connection"""
    try:
        # Test basic connection
        conn = getopenconnection()
        print("PostgreSQL connection successful")
        
        # Check/create database
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='dds_assgn1'")
        
        if cur.fetchone()[0] == 0:
            cur.execute("CREATE DATABASE dds_assgn1")
            print("Database 'dds_assgn1' created")
        else:
            print("Database 'dds_assgn1' exists")
        
        cur.close()
        conn.close()
        
        # Test assignment database connection
        conn = getopenconnection(dbname='dds_assgn1')
        print("Connected to dds_assgn1 database")
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")
        return False
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    test_connection()