#!/usr/bin/env python3
"""
Load full ratings.dat and create partitions
"""
import time
import os
from Interface import *

def load_full_ratings_and_partition():
    """Load full ratings.dat and create partitions"""
    
    if not os.path.exists('ratings.dat'):
        print("❌ File ratings.dat không tồn tại!")
        return
    
    file_size = os.path.getsize('ratings.dat') / (1024 * 1024)  # MB
    print(f"📁 File ratings.dat: {file_size:.1f} MB")
    
    try:
        print("🚀 Bắt đầu xử lý dữ liệu đầy đủ ratings.dat...")
        print("=" * 60)
        
        # Connect to database
        conn = getopenconnection(dbname='dds_assgn1')
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        # 1. XÓA TẤT CẢ BẢNG HIỆN CÓ
        print("🧹 Bước 1: Xóa tất cả bảng hiện có...")
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = [row[0] for row in cur.fetchall()]
        
        if tables:
            print(f"   Đang xóa {len(tables)} bảng...")
            for table in tables:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                print(f"     ✓ Đã xóa {table}")
        else:
            print("   Không có bảng nào để xóa")
        cur.close()
        
        # 2. LOAD DỮ LIỆU TỪ RATINGS.DAT
        print("\n📥 Bước 2: Load dữ liệu từ ratings.dat...")
        start_time = time.time()
        loadratings('ratings', 'ratings.dat', conn)
        load_time = time.time() - start_time
        
        # Đếm số records đã load
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ratings")
        total_records = cur.fetchone()[0]
        cur.close()
        
        print(f"   ✅ Load hoàn thành!")
        print(f"   📊 Đã load: {total_records:,} records")
        print(f"   ⏱️  Thời gian: {load_time:.1f} giây")
        print(f"   🚀 Tốc độ: {total_records/load_time:,.0f} records/giây")
        
        # 3. TẠO RANGE PARTITIONS
        print(f"\n📊 Bước 3: Tạo Range Partitions...")
        num_partitions = 5
        print(f"   Tạo {num_partitions} range partitions dựa trên rating (0-5)...")
        
        start_time = time.time()
        rangepartition('ratings', num_partitions, conn)
        partition_time = time.time() - start_time
        
        print(f"   ✅ Range partitioning hoàn thành!")
        print(f"   ⏱️  Thời gian: {partition_time:.1f} giây")
        
        # Hiển thị phân bố dữ liệu trong range partitions
        cur = conn.cursor()
        print(f"   📈 Phân bố dữ liệu Range Partitions:")
        delta = 5.0 / num_partitions
        total_in_partitions = 0
        
        for i in range(num_partitions):
            cur.execute(f"SELECT COUNT(*) FROM range_part{i}")
            count = cur.fetchone()[0]
            total_in_partitions += count
            
            minRange = i * delta
            maxRange = minRange + delta
            if i == 0:
                range_desc = f"[{minRange:.1f}, {maxRange:.1f}]"
            else:
                range_desc = f"({minRange:.1f}, {maxRange:.1f}]"
            
            percentage = (count / total_records) * 100
            print(f"     range_part{i}: {range_desc} -> {count:,} records ({percentage:.1f}%)")
        
        cur.close()
        
        # 4. TẠO ROUND ROBIN PARTITIONS
        print(f"\n🔄 Bước 4: Tạo Round Robin Partitions...")
        print(f"   Tạo {num_partitions} round robin partitions...")
        
        start_time = time.time()
        roundrobinpartition('ratings', num_partitions, conn)
        partition_time = time.time() - start_time
        
        print(f"   ✅ Round robin partitioning hoàn thành!")
        print(f"   ⏱️  Thời gian: {partition_time:.1f} giây")
        
        # Hiển thị phân bố dữ liệu trong round robin partitions
        cur = conn.cursor()
        print(f"   📈 Phân bố dữ liệu Round Robin Partitions:")
        
        for i in range(num_partitions):
            cur.execute(f"SELECT COUNT(*) FROM rrobin_part{i}")
            count = cur.fetchone()[0]
            percentage = (count / total_records) * 100
            print(f"     rrobin_part{i}: {count:,} records ({percentage:.1f}%)")
        
        cur.close()
        
        # 5. KIỂM TRA KẾT QUẢ CUỐI CÙNG
        print(f"\n📋 Bước 5: Tổng kết...")
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
        final_tables = cur.fetchall()
        
        print(f"   🗄️  Tổng số bảng đã tạo: {len(final_tables)}")
        for table in final_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cur.fetchone()[0]
            print(f"     ✓ {table[0]}: {count:,} records")
        
        cur.close()
        conn.close()
        
        print("\n" + "=" * 60)
        print("🎉 HOÀN THÀNH! Dữ liệu đầy đủ đã được load và phân mảnh thành công!")
        print("💡 Bây giờ bạn có thể xem các bảng trong pgAdmin:")
        print("   - ratings: Bảng chính chứa tất cả dữ liệu")
        print("   - range_part0-4: Phân mảnh theo khoảng rating")
        print("   - rrobin_part0-4: Phân mảnh round robin")
        print("   - partition_metadata: Metadata về các partition")
        
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_full_ratings_and_partition()
