#!/usr/bin/env python3
"""
Clean database - xóa tất cả bảng hiện có để bắt đầu từ đầu
"""
import psycopg2
from Interface import getopenconnection

def clean_database():
    """Xóa tất cả bảng trong database dds_assgn1"""
    try:
        print("🧹 Bắt đầu dọn dẹp database...")
        print("=" * 50)
        
        # Kết nối đến database
        conn = getopenconnection(dbname='dds_assgn1')
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Lấy danh sách tất cả bảng hiện có
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
        tables = cur.fetchall()
        
        if not tables:
            print("✅ Database đã sạch - không có bảng nào để xóa")
        else:
            print(f"📋 Tìm thấy {len(tables)} bảng cần xóa:")
            for table in tables:
                print(f"   - {table[0]}")
            
            print(f"\n🗑️  Đang xóa {len(tables)} bảng...")
            
            # Xóa từng bảng
            for table in tables:
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")
                    print(f"   ✅ Đã xóa: {table[0]}")
                except Exception as e:
                    print(f"   ❌ Lỗi khi xóa {table[0]}: {e}")
            
            # Kiểm tra lại sau khi xóa
            cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
            remaining_tables = cur.fetchone()[0]
            
            if remaining_tables == 0:
                print(f"\n🎉 Thành công! Đã xóa tất cả bảng.")
            else:
                print(f"\n⚠️  Còn lại {remaining_tables} bảng chưa xóa được.")
        
        cur.close()
        conn.close()
        
        print("\n" + "=" * 50)
        print("✅ Database đã được dọn dẹp hoàn toàn!")
        print("💡 Bây giờ bạn có thể:")
        print("   - Chạy Assignment1Tester.py để test với dữ liệu mẫu")
        print("   - Chạy demo.py để demo các chức năng")
        print("   - Chạy load_full_data.py để load dữ liệu đầy đủ")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_database()
