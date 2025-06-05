# Hệ thống Phân mảnh Dữ liệu Phân tán - Distributed Database Partitioning System

## Tổng quan
Dự án này thực hiện mô phỏng các phương pháp phân mảnh dữ liệu ngang (horizontal partitioning) trên PostgreSQL, sử dụng dữ liệu đánh giá phim từ MovieLens dataset.

## Cấu hình Hệ thống
- **Database**: PostgreSQL 17.x
- **Language**: Python 3.12.x
- **OS**: Windows 10/11 hoặc Ubuntu
- **Dependencies**: psycopg2

## Cài đặt và Cấu hình

### 1. Cài đặt PostgreSQL
- Tải và cài đặt PostgreSQL từ https://www.postgresql.org/download/
- Tạo database với thông tin:
  - Username: `postgres`
  - Password: `1234`
  - Host: `localhost`
  - Port: `5432`

### 2. Cài đặt Python Dependencies
```bash
pip install psycopg2-binary
```

### 3. Kiểm tra Kết nối
```bash
python test_connection.py
```

## Cấu trúc File

### File Chính
- `Interface.py` - Chứa các hàm chính để thực hiện partitioning
- `Assignment1Tester.py` - Script test chính của bài tập
- `testHelper.py` - Các hàm hỗ trợ testing

### File Dữ liệu
- `ratings.dat` - Dữ liệu MovieLens chính (10M records)
- `test_data.dat` - Dữ liệu test mẫu (20 records)

### File Test và Demo
- `test_connection.py` - Kiểm tra kết nối PostgreSQL
- `demo.py` - Demo tổng quan hệ thống
- `performance_test.py` - Test hiệu suất
- `test_full_ratings.py` - Test với dữ liệu đầy đủ

## Các Chức năng Chính

### 1. Load Dữ liệu - `loadratings()`
```python
loadratings(table_name, file_path, connection)
```
- Tải dữ liệu từ file `.dat` vào PostgreSQL
- Tự động parse format `UserID::MovieID::Rating::Timestamp`
- Tối ưu hóa cho file lớn với indexes và COPY command

### 2. Range Partitioning - `rangepartition()`
```python
rangepartition(table_name, num_partitions, connection)
```
- Phân chia dữ liệu dựa trên khoảng giá trị Rating (0-5)
- Tạo các partition với ranges đều nhau
- Ví dụ với 3 partitions:
  - `range_part0`: [0, 1.67]
  - `range_part1`: (1.67, 3.34]
  - `range_part2`: (3.34, 5]

### 3. Round Robin Partitioning - `roundrobinpartition()`
```python
roundrobinpartition(table_name, num_partitions, connection)
```
- Phân chia dữ liệu theo kiểu vòng tròn
- Đảm bảo load balancing giữa các partitions
- Record thứ i đi vào partition `i % num_partitions`

### 4. Range Insert - `rangeinsert()`
```python
rangeinsert(table_name, userid, movieid, rating, connection)
```
- Chèn record mới vào partition phù hợp dựa trên Rating
- Tự động xác định partition đích

### 5. Round Robin Insert - `roundrobininsert()`
```python
roundrobininsert(table_name, userid, movieid, rating, connection)
```
- Chèn record mới theo kiểu round robin
- Dựa trên tổng số record hiện tại

## Hướng dẫn Sử dụng

### Demo Cơ bản
```bash
python demo.py
```

### Test với Dữ liệu Đầy đủ
```bash
python demo.py --full
```

### Chạy Test Chính thức
```bash
python Assignment1Tester.py
```

### Test Hiệu suất
```bash
python performance_test.py
```

## Tối ưu hóa Hiệu suất

### 1. Indexes
- Tự động tạo index trên các cột thường dùng
- `rating`, `userid`, `movieid`

### 2. Batch Operations
- Sử dụng `COPY` command cho bulk insert
- Tối ưu query với prepared statements

### 3. Connection Management
- Connection pooling cho môi trường production
- Transaction management với rollback

### 4. Memory Management
- Streaming processing cho file lớn
- Batch size optimization

## Kết quả Test

### Dữ liệu Mẫu (20 records)
- Load time: < 0.1 seconds
- Partition time: < 0.05 seconds per partition
- Insert time: < 0.01 seconds per operation

### Dữ liệu Đầy đủ (10M records)
- Load time: ~15-30 seconds
- Range partition: ~5-15 seconds
- Round robin partition: ~10-20 seconds
- Insert operations: < 0.01 seconds

## Lưu ý Quan trọng

### 1. Tuân thủ Yêu cầu
- ✅ Không sử dụng biến global (sử dụng metadata table)
- ✅ Không sửa đổi file dữ liệu
- ✅ Không hardcode tên file hoặc database
- ✅ Không đóng connection trong hàm
- ✅ Schema table đúng như yêu cầu

### 2. Naming Convention
- Range partitions: `range_part0`, `range_part1`, ...
- Round robin partitions: `rrobin_part0`, `rrobin_part1`, ...

### 3. Error Handling
- Robust error handling với rollback
- Detailed error messages
- Graceful degradation

## Troubleshooting

### Connection Issues
```bash
# Kiểm tra PostgreSQL service
net start postgresql-x64-17

# Test connection
python test_connection.py
```

### Memory Issues với File Lớn
- Tăng `shared_buffers` trong postgresql.conf
- Tăng `work_mem` cho sorting operations
- Monitor với `pg_stat_activity`

### Performance Issues
- Chạy `VACUUM ANALYZE` sau bulk operations
- Kiểm tra query plans với `EXPLAIN`
- Monitor index usage

## Tác giả
Được phát triển cho Bài tập lớn Cơ sở dữ liệu phân tán

## License
Educational use only
