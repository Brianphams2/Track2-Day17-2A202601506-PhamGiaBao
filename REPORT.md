# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Phạm Gia Bảo  **Lớp:** AICB-P2T2  **Mã SV:** 2A202601506  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details open>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 48.5s
  run 2/3 … 31.9s
  run 3/3 … 34.0s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (và đạt cả 2 bài mở rộng A + B: **110/100**)

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` bị phình to bất thường sau mỗi lần chạy lại pipeline (tăng từ 12.480 lên 38.750 hàng sau 3 run, thừa 26.270 hàng); checksum thay đổi giữa các run (`7c461563f4` → `d11657ff21` → `2b76a4f850`); có 12.480 ticket bị trùng lặp nhiều hàng. |
| **Nguyên nhân** | Model incremental trong dbt thiếu khai báo `unique_key` và `incremental_strategy`, khiến dbt mặc định sinh câu lệnh ghi `INSERT` (append-only) thay vì upsert/merge. Khi chạy lại pipeline, retry, hoặc khi một ticket có nhiều bản ghi CDC cập nhật (`op = 'u'`) ở các ngày khác nhau lọt qua bộ lọc `run_date`, dbt chèn thêm các hàng mới thay vì cập nhật theo natural key (`ticket_id`). Đồng thời trong DAG Airflow, `catchup=True` và thiếu `max_active_runs=1` gây nguy cơ tự động trigger nhiều run đồng thời ghi vào cùng một bảng. |
| **Cách khắc phục** | 1. `dbt/models/gold/gold_training_set.sql`: Khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` trong `config()`.<br>2. `dags/ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38.750 hàng (12.480 ticket bị lặp, checksum không ổn định) · sau: 12.480 hàng (1 hàng / 1 ticket, 0 lặp) · checksum 3 lượt: `8dd7c98653` (ổn định 100%). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` chỉ có 8.645 hàng, thiếu 455 hàng so với kỳ vọng 9.100 hàng (14 ngày × 650 khách hàng), các tổ hợp `(event_date, customer_id)` bị thiếu tập trung ở các ngày trong quá khứ. |
| **P99 độ trễ đo được** | **2.73 ngày** *(chính xác: P50 = 0.13 ngày, P95 = 1.81 ngày, P99 = 2.7258 ngày, max = 2.9447 ngày; tỷ lệ event đến trễ > 1 ngày là 5.05%)* |
| **Lookback đã chọn** | 3 ngày — vì P99 độ trễ thực tế đo được là 2.73 ngày (và max = 2.94 ngày), làm tròn lên số ngày vận hành nguyên gần nhất là 3 ngày để bao phủ toàn bộ các late-arriving events mà không quét dư thừa dữ liệu lịch sử. |
| **Nguyên nhân** | Điều kiện incremental cũ `where event_date > (select max(event_date) from {{ this }})` là một watermark tuyệt đối tiến về phía trước. Các event phát sinh ở ngày cũ nhưng tới kho muộn (late arrivals do network/queue delay) có `event_date` nhỏ hơn `max(event_date)` đã có trong target, do đó bị loại bỏ hoàn toàn khỏi điều kiện lọc và không bao giờ được tổng hợp ở các lượt chạy sau. |
| **Cách khắc phục** | 1. `dbt/models/gold/gold_feature_daily.sql`: Mở rộng điều kiện incremental thành cửa sổ lùi `where event_date >= (select max(event_date) - interval 3 day from {{ this }})`.<br>2. Thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` để ghi đè các tổ hợp được tính toán lại trong cửa sổ lookback, tránh bị cộng dồn số liệu qua các run. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455 hàng) · sau: 9.100 hàng (đủ 9.100 / 9.100 hàng, checksum 3 lượt: `3db448685c` ổn định 100%). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 đại diện cho phân bố độ trễ của đại đa số dữ liệu thực tế (99% lượng event), giúp xác định lookback window tối ưu (3 ngày) vừa đủ bao phủ toàn bộ các event đến muộn có tính chu kỳ/vận hành mà chi phí tính toán tăng thêm ở mỗi run là tối thiểu và cố định. Nếu chọn `max`, window có thể bị kéo dài quá mức bởi một vài outlier hiếm gặp (ví dụ lỗi mạng kẹt hàng tháng), buộc pipeline ở mọi chu kỳ hàng ngày đều phải quét, gom nhóm và ghi đè một lượng dữ liệu lịch sử khổng lồ, gây lãng phí nghiêm trọng tài nguyên I/O và CPU. Với các trường hợp ngoại lệ vượt quá P99, giải pháp chuẩn trong kiến trúc dữ liệu là sử dụng quy trình backfill định kỳ theo batch thay vì ép pipeline streaming/daily phải chịu tải thường trực.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Cột `silver_tickets.priority` có 6.606 hàng sai contract (nhiều giá trị `NULL` và các số ngoài miền 1..4 như `0`, `5`, `-1`); bảng `quarantine_tickets` rỗng (0 / 312 hàng lỗi); contract bị tắt (`enforced: false`). |
| **Nguyên nhân** | Nguồn CDC xảy ra hiện tượng schema evolution từ ngày 2026-08-10: backend chuyển cách biểu diễn `priority` từ số sang nhãn chữ (`urgent`, `high`, `medium`, `low`). Logic cũ dùng `try_cast(priority_raw as integer)` làm mất toàn bộ các nhãn chữ hợp lệ (biến thành `NULL`), đồng thời lại nhận nhầm các số không hợp lệ (`0`, `5`, `-1`). Ngoài ra, nếu xếp hạng `row_number()` trước khi lọc thì một ticket có bản ghi update mới nhất bị lỗi sẽ bị loại bỏ hoàn toàn khỏi Silver thay vì giữ lại trạng thái hợp lệ liền trước. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ** (`'1'`, `'2'`, `'3'`, `'4'`): Giữ nguyên giá trị integer 1..4.<br>2. **Nhãn chuỗi hợp lệ** (`'urgent'`, `'high'`, `'medium'`, `'low'`): Map tương ứng về số nguyên (`urgent→1`, `high→2`, `medium→3`, `low→4`).<br>3. **Dữ liệu lỗi thật** (`'P1'`, `'P2'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `NULL`): Trả về `NULL` để chuyển vào bảng `quarantine_tickets`. |
| **Cách khắc phục** | 1. `dbt/macros/normalize_priority.sql`: Viết khối `CASE` chuẩn hóa chính xác 3 nhóm trên, trả về `NULL` cho nhóm 3; bổ sung macro lý do loại `priority_reject_reason`.<br>2. `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi lỗi trước (`where normalize_priority(...) is not null`), sau đó mới đánh số thứ tự `row_number()` để lấy trạng thái hợp lệ mới nhất của ticket.<br>3. `dbt/models/silver/quarantine_tickets.sql`: Lấy các bản ghi lỗi với điều kiện `where normalize_priority(priority_raw) is null`.<br>4. `dbt/models/silver/schema.yml`: Đặt `contract.enforced: true`, thêm dbt test `not_null` và `accepted_values: [1, 2, 3, 4]` (`quote: false`). |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (đúng 312 / 312 bản ghi CDC lỗi, checksum 3 lượt: `ebb89036fb` ổn định) · `silver_tickets` đủ 12.480 ticket · `dbt test` 11/11 pass · `silver_tickets.priority ∈ 1..4, không NULL`: sạch (0 hàng lỗi). |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> 1. **Nên chặn ở Silver, không chặn ở Bronze**: Tầng Bronze đóng vai trò là kho lưu trữ dữ liệu thô bất biến (raw immutable ingestion layer). Nếu chặn hoặc loại bỏ dữ liệu ngay tại Bronze, ta sẽ đánh mất dấu vết gốc, gây cản trở nghiêm trọng cho việc điều tra sự cố (root-cause analysis), kiểm toán dữ liệu (auditing), và không thể tái xử lý (replay) khi logic nghiệp vụ hoặc contract được cập nhật. Do đó, Bronze phải tiếp nhận toàn bộ dữ liệu as-is, việc làm sạch, kiểm tra contract và phân luồng cách ly (quarantine) thuộc trách nhiệm của tầng Silver.
> 2. **Không để pipeline dừng khi gặp bản ghi lỗi**: Trong môi trường sản xuất thực tế với khối lượng dữ liệu lớn, một tỷ lệ nhỏ bản ghi bị lỗi định dạng (312 bản ghi lỗi trên tổng số 14.300 bản ghi CDC, trong khi pipeline còn phục vụ 129.462 events và 31.200 document chunks hợp lệ) không thể làm tắc nghẽn toàn bộ luồng xử lý. Cơ chế Quarantine Pattern giúp cách ly an toàn dữ liệu hỏng vào một hàng đợi riêng để đội ngũ vận hành kiểm tra và xử lý sau, trong khi pipeline vẫn liên tục phục vụ dữ liệu sạch cho các ứng dụng downstream và mô hình AI.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

### Bài A — Query dashboard chậm (+5 điểm)

| | |
|---|---|
| **Bài đã làm** | Bài A (Tối ưu Dashboard & Small-file Problem) |
| **Triệu chứng** | Truy vấn `queries/dashboard.sql` quét 5.000.000 `rows scanned`, phải đọc qua 5.000 file Parquet nhỏ rời rạc không partition, thời gian chạy ~10,6 giây. |
| **Nguyên nhân** | 1. **Small-file problem**: Dataset `data/gold_events/` bị phân mảnh thành 5.000 file nhỏ (~26 hàng/file). DuckDB đọc Parquet theo lô vector hóa và làm tròn lên cho từng file (~1.000 hàng/file), khiến tổng công quét bị đội lên 5.000.000 đơn vị.<br>2. **Layout thiếu Partitioning**: Đường dẫn không chứa thông tin ngày hay khách hàng nên engine bắt buộc phải mở toàn bộ 5.000 file.<br>3. **Predicate không sargable**: `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` bọc cột trong hàm `strftime`, vô hiệu hóa partition pruning và row-group min/max statistics. |
| **Cách khắc phục** | 1. `tools/compact.py`: Dùng `COPY ... TO 'data/gold_events_v2'` để compact 5.000 file nhỏ thành 14 file lớn theo `PARTITION_BY (event_date)`, sắp xếp `ORDER BY event_date, customer_name, event_time`, đặt `ROW_GROUP_SIZE 2048` để gom các hàng của cùng khách hàng vào các row group liền kề.<br>2. `queries/dashboard.sql`: Trỏ vào `data/gold_events_v2/**/*.parquet` với `hive_partitioning = true`, viết lại filter dạng sargable (`event_date = '2026-08-09' and event_time >= timestamp '2026-08-09 00:00:00' and event_time < timestamp '2026-08-10 00:00:00'`). |
| **Bằng chứng** | • **rows scanned**: 5.000.000 → 9.324 (giảm **536.3×**, yêu cầu $\ge 10\times$)<br>• **số file parquet**: 5.000 → 14 file<br>• **rows on disk**: 130.683 → 130.683 (giữ nguyên)<br>• **result hash**: `4379e4c5d9f3` → `4379e4c5d9f3` (kết quả không đổi)<br>• **thời gian chạy**: 10.610,7 ms → 43,1 ms |

---

### Bài B — Consumer gặp sự cố giữa batch (+5 điểm)

| | |
|---|---|
| **Bài đã làm** | Bài B (Consumer Crash Safety & Idempotent Processing) |
| **Triệu chứng** | Khi tiến trình consumer bị crash ở giữa lô (tại batch 7 trong kịch bản `tools/crash_test.py`), hệ thống bị **mất 500 bản ghi** (`lost = 500`), tổng số hàng sau restart chỉ đạt 19.500 / 20.000 hàng. |
| **Nguyên nhân** | Consumer sử dụng ngữ nghĩa **at-most-once**: commit offset trước khi ghi dữ liệu xuống database (`consumer.commit()` trước `write_batch()`). Khi tiến trình chết tại `maybe_crash()`, offset đã bị dịch chuyển lên 3.500 nhưng dữ liệu lô 7 chưa được lưu; khi restart, consumer đọc tiếp từ offset 3.500, dẫn đến mất trắng 500 bản ghi của lô 7. |
| **Cách khắc phục** | 1. Đổi thứ tự trong `ingest/consumer.py` sang ngữ nghĩa **at-least-once**: Ghi dữ liệu xuống DB trước (`write_batch()`), sau đó mới commit offset (`consumer.commit()`).<br>2. Đảm bảo tính **Idempotent Write**: Thêm ràng buộc `PRIMARY KEY (event_id)` vào DDL bảng `bronze_events_stream`, và đổi câu lệnh ghi sang `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...` để khi replay lô chưa commit sau crash, dữ liệu được cập nhật idempotent thay vì sinh bản ghi trùng lặp. |
| **Bằng chứng** | • `tools/crash_test.py`: `BÀI MỞ RỘNG B: ĐẠT ✓`<br>• **Không mất bản ghi**: ✓ (`lost = 0`)<br>• **Không trùng bản ghi**: ✓ (`dup = 0`)<br>• **C == A**: ✓ (20.000 == 20.000 hàng, 20.000 distinct `event_id`)<br>• **Phân tích cơ chế**: `DO UPDATE` khác `DO NOTHING` ở chỗ: nếu một message được phát lại (replay) nhưng mang nội dung/trạng thái mới cập nhật, `DO UPDATE` đảm bảo kho dữ liệu luôn phản ánh trạng thái mới nhất của message thay vì bỏ qua silently. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra cấu hình `materialized = 'incremental'` của các dbt model: đã khai báo đúng natural key trong `unique_key` và chiến lược `incremental_strategy` (merge / delete+insert) phù hợp với grain của bảng hay chưa; đồng thời kiểm tra tham số DAG Airflow (`catchup = False`, `max_active_runs = 1`) để đảm bảo tính idempotent khi chạy lại hoặc retry. |
| 2 | Phân tích phân bố độ trễ dữ liệu (`_ingested_at - event_time`) để xác định P95/P99, kiểm tra điều kiện lọc incremental xem có nguy cơ bỏ sót late-arriving data do watermark tuyệt đối hay không, và thiết lập lookback window thích hợp kết hợp với composite unique key để tránh trùng lặp số liệu. |
| 3 | Kiểm tra Data Contract tại các điểm giao tiếp giữa các hệ thống (schema evolution, kiểu dữ liệu, miền giá trị); thiết lập macro chuẩn hóa tập trung, áp dụng cơ chế Quarantine Pattern để cách ly dữ liệu lỗi mà không làm dừng pipeline; đồng thời đảm bảo thứ tự lọc dữ liệu lỗi trước khi ranking trạng thái thực thể. |
| Bonus A | Kiểm tra layout lưu trữ Parquet (small-file problem, partition strategy) và cấu trúc điều kiện WHERE của các truy vấn phân tích (đảm bảo predicate sargable để kích hoạt partition pruning và row-group min/max statistics pushdown). |
| Bonus B | Kiểm tra delivery semantics của consumer và tính idempotent của tầng storage: chuyển từ at-most-once sang at-least-once (ghi dữ liệu thành công trước khi commit offset) kết hợp với `ON CONFLICT DO UPDATE` theo unique natural key để triệt tiêu trùng lặp và không làm mất dữ liệu khi gặp sự cố crash. |
