#!/usr/bin/env python3
"""
Check tables in database
"""
import psycopg2
from Interface import getopenconnection

def check_tables():
    try:
        conn = getopenconnection(dbname='dds_assgn1')
        cur = conn.cursor()
        
        # Kiểm tra tất cả bảng hiện có
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
        tables = cur.fetchall()
        
        print('🗄️  Các bảng hiện có trong database dds_assgn1:')
        if tables:
            for table in tables:
                # Lấy số lượng records trong mỗi bảng
                cur.execute(f'SELECT COUNT(*) FROM {table[0]}')
                count = cur.fetchone()[0]
                print(f'  ✓ {table[0]}: {count} records')
        else:
            print('  ❌ Không có bảng nào!')
        
        print(f'\n📊 Tổng số bảng: {len(tables)}')
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f'❌ Lỗi: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_tables()
