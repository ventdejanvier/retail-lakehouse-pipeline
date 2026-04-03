# Retail-Data-Lakehouse-Pipeline

## Giới thiệu
Đề tài tập trung xây dựng hệ thống Data Lakehouse hiện đại theo kiến trúc Medallion (Bronze - Silver - Gold) nhằm quản lý và phân tích dữ liệu bán lẻ trực tuyến. Hệ thống kết hợp sức mạnh xử lý của Apache Spark và khả năng lưu trữ mạnh mẽ của Azure Data Lake Storage (ADLS) Gen2.

## Tập dữ liệu
Đề tài sử dụng tập dữ liệu Online Retail II chứa toàn bộ giao dịch xảy ra trong 2 năm của một công ty bán lẻ trực tuyến tại Anh.
* **Nguồn**: UCI Machine Learning Repository - Online Retail II
* **Kích thước**: ~1.07 triệu dòng giao dịch.
* **Định dạng**: CSV.

## Kiến trúc hệ thống 
1. **Ingestion Layer:** Thu thập dữ liệu từ Local HDFS nạp lên Azure Bronze.
2. **Storage Layer:** Lưu trữ tập trung trên Azure Data Lake Storage (ADLS) Gen2.
3. **Metadata Layer:** Quản lý bảng với định dạng Apache Iceberg và Apache Atlas.
4. **Processing Layer:** Xử lý dữ liệu lớn với Apache Spark 3.5 & Trino.
5. **Consumption Layer:** Trực quan hóa qua hệ thống Dashboard chuyên sâu trên Power BI.

## Cấu trúc thư mục
```bash
Retail-Data-Lakehouse-Pipeline/
├── docker/         # Cấu hình hạ tầng hệ thống
├── data/           # Chứa tập dữ liệu gốc online_retail_II.csv
├── bronze/         # Scripts nạp dữ liệu từ HDFS Local lên Azure Bronze 
├── silver/         # Scripts làm sạch, lọc rác và chuẩn hóa kỹ thuật  
├── gold/           # Scripts RFM, Scaling và các mô hình ML 
├── audit/          # Scripts kiểm tra chất lượng dữ liệu giữa các tầng
├── power-bi/       # File báo cáo .pbix và thiết kế Dashboard
├── docs/           # Sơ đồ kiến trúc và ảnh dashboard phân tích
├── .env.example    # File mẫu cấu hình bảo mật Azure
└── README.md       # Tài liệu hướng dẫn tổng thể dự án
```

## Thành viên thực hiện
| STT | MSSV | Họ và tên sinh viên |
| :--- | :--- | :--- | 
| 1 | 23133001 | Huỳnh Tú An |
| 2 | 22133044 | Trần Thị Kim Phượng |
| 3 | 22110398 | Ka Phúc |


## Hướng dẫn cài đặt nhanh
1. **Clone dự án:** 
   `git clone https://github.com/your-username/Retail-Data-Lakehouse-Pipeline.git`

2.  **Cấu hình môi trường:** Tạo file `.env` và điền thông tin Azure Credentials theo mẫu `.env.example`.

3. Sử dụng hệ thống Cloud chung

*   Dự án sử dụng chung một tài khoản Microsoft Fabric để tối ưu hóa tài nguyên và chi phí.
*   **Thành viên:** Đăng nhập theo thông tin tài khoản nhóm cung cấp tại [Microsoft Fabric](https://app.fabric.microsoft.com/).
*   **Workspace:** `Nhom5_Retail_Analytics_Workspace`.

