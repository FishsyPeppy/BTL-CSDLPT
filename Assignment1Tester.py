import psycopg2
import traceback
import testHelper
import Interface as MyAssignment
import time

# Constants
DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10000054

def print_progress(message, indent=0):
    """Print progress message with timestamp and indentation"""
    print(f"[{time.strftime('%H:%M:%S')}] {'  ' * indent}{message}")

def verify_partition_content(conn, prefix, number_of_partitions):
    """Verify content of partition tables"""
    cur = conn.cursor()
    total_rows = 0
    print_progress(f"Verifying {prefix} partition tables:")
    for i in range(number_of_partitions):
        table_name = f"{prefix}{i}"
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        total_rows += count
        print_progress(f"- {table_name}: {count} rows", indent=1)
    
    cur.execute(f"SELECT COUNT(*) FROM {RATINGS_TABLE}")
    original_count = cur.fetchone()[0]
    print_progress(f"Total rows: partitions={total_rows}, original={original_count}")
    print_progress("Partition content passed!" if total_rows == original_count else "Partition content failed!")
    cur.close()

def main():
    try:
        print_progress("Starting test...")
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            testHelper.deleteAllPublicTables(conn)

            # Test loadratings
            print_progress("Testing loadratings...")
            start_time = time.time()
            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            load_time = time.time() - start_time
            print_progress(f"loadratings: {'passed' if result else 'failed'}! ({load_time:.3f} seconds)")

            # Get partition choice
            partition_choice = input("\nChoose partitioning (range/roundrobin): ").strip().lower()
            start_time = time.time()

            if partition_choice == 'range':
                print_progress("Testing RANGE partitioning...")
                print_progress("Creating 5 range partitions...")
                [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
                if result:
                    print_progress("rangepartition passed!")
                    verify_partition_content(conn, RANGE_TABLE_PREFIX, 5)
                else:
                    print_progress("rangepartition failed!")

                print_progress("Testing range insert...")
                [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
                print_progress(f"rangeinsert: {'passed' if result else 'failed'}!")

            elif partition_choice == 'roundrobin':
                print_progress("Testing ROUND ROBIN partitioning...")
                print_progress("Creating 5 roundrobin partitions...")
                [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
                if result:
                    print_progress("roundrobinpartition passed!")
                    verify_partition_content(conn, RROBIN_TABLE_PREFIX, 5)
                else:
                    print_progress("roundrobinpartition failed!")

                print_progress("Testing roundrobin insert...")
                [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '4')
                print_progress(f"roundrobininsert: {'passed' if result else 'failed'}!")

            else:
                print_progress("Invalid choice! Choose 'range' or 'roundrobin'.")
                return

            # Display total execution time
            elapsed_time = time.time() - start_time
            print_progress(f"Total partitioning + insert time: {elapsed_time:.3f} seconds")

            # Delete tables
            if input('\nPress enter to delete all tables: ') == '':
                print_progress("Deleting all tables...")
                testHelper.deleteAllPublicTables(conn)
                print_progress("Tables deleted.")

    except Exception:
        print_progress("Error occurred:")
        traceback.print_exc()

if __name__ == '__main__':
    main()