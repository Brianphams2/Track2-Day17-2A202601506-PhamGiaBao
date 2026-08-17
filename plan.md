# Kế hoạch hoàn thành LAB 17 - mục tiêu 110/100

## 1. Phạm vi và kết quả cuối

Hoàn thành ba nhiệm vụ chính để giữ chắc 100/100, sau đó hoàn thành hai bài mở
rộng, mỗi bài +5 điểm. Bonus không được làm regression phần chính.

| Hạng mục | Kết quả bắt buộc |
|---|---:|
| `gold_training_set` | 12.480 hàng, 1 hàng / `ticket_id` |
| `gold_feature_daily` | 9.100 hàng |
| `gold_doc_chunks` | 31.200 hàng, ổn định |
| `quarantine_tickets` | 312 hàng, 1 hàng / 1 row CDC lỗi |
| `silver_tickets` | 12.480 ticket |
| `silver_tickets.priority` | không NULL, chỉ thuộc 1..4 |
| `dbt test` | pass, tổng số test lớn hơn 9 |
| Tính ổn định | 4 checksum giống nhau trong 3 lượt |
| Bonus A | scan giảm >=10x, file giảm, hash không đổi |
| Bonus B | crash-test đạt, không mất/trùng row |

## 2. Rule bắt buộc

### Được phép sửa

- `dbt/`, `dags/`, `ingest/`, `queries/`, `tools/compact.py`.
- `REPORT.md` và `plan.md`.

### Tuyệt đối không sửa

- `expected/`
- `seed/generate.py`
- `tools/verify.py`
- `tools/explain.py`
- `tools/common.py`

Không xóa/sửa dữ liệu nguồn để ép số hàng, không hard-code expected/checksum,
không sửa công cụ chấm. Không nộp `.venv/`, `warehouse.duckdb` hoặc `data/`.

### Invariant

- Training grain: 1 row / ticket.
- Feature grain: 1 row / (`event_date`, `customer_id`).
- Quarantine grain: 1 row / row CDC lỗi, không deduplicate theo ticket.
- Bronze giữ nguyên dữ liệu thô để audit/replay.
- Chỉ loại row CDC lỗi; ticket còn trạng thái hợp lệ phải được giữ lại.
- `gold_doc_chunks` là nhóm đối chứng, không được ảnh hưởng.

## 3. Baseline trước sửa

### Input

- Repo gốc, seed và môi trường từ `make setup`.

### Logic

1. Kiểm tra `git status`.
2. Chạy `make quick`, sau đó `make verify`.
3. Lưu row count, checksum, test count và output trước sửa.
4. Đo P50/P95/P99/max của `_ingested_at - event_time`.
5. Query duplicate training, cặp feature bị thiếu và phân bố `priority_raw`.

### Output

- Baseline có số liệu thực đo cho report.
- P99 làm căn cứ chọn lookback.

## 4. Nhiệm vụ 1 - Training idempotent

### Input

- `silver_tickets`, CDC updates, `run_date`.
- `dbt/models/gold/gold_training_set.sql`.
- `dags/ai_training_pipeline.py`.

### Logic chuẩn

1. Giữ filter theo `run_date`.
2. Dùng `unique_key = 'ticket_id'`.
3. Dùng `incremental_strategy = 'merge'`.
4. Không delete-and-insert theo ngày vì ticket có thể xuất hiện ở nhiều ngày.
5. Đặt DAG `catchup = False`, `max_active_runs = 1`.

### Root cause

Incremental model thiếu key/strategy nên dbt append khi rerun/retry thay vì
update entity theo `ticket_id`. DAG parameters chỉ giảm khả năng kích hoạt lỗi,
không thay thế việc sửa model.

### Output

- 12.480 hàng, không lặp ticket, checksum ổn định; DAG đạt AST check.

## 5. Nhiệm vụ 2 - Late-arriving events

### Input

- `silver_events`, phân bố độ trễ và P99.
- `dbt/models/gold/gold_feature_daily.sql`.

### Logic chuẩn

1. Chọn lookback từ P99 thực đo, làm tròn theo ngày vận hành.
2. Tính lại cửa sổ lùi từ `max(event_date)` thay vì chỉ lấy ngày mới hơn.
3. Dùng composite key `['event_date', 'customer_id']`.
4. Dùng strategy replace/upsert để row trong lookback không cộng dồn.
5. Không coi việc đổi `>` thành `>=` là giải pháp đầy đủ.

### Root cause

Watermark tuyệt đối theo max event date bỏ vĩnh viễn event ngày cũ được ingest
muộn. Lookback dài hơn giảm bỏ sót nhưng tăng chi phí ở mọi lượt chạy.

### Output

- 9.100 hàng, không trùng composite grain, checksum ổn định.
- Report ghi P99 = 2,7258 ngày và lookback 3 ngày.

## 6. Nhiệm vụ 3 - Priority contract và quarantine

### Input

- `bronze_tickets_cdc.priority_raw`.
- Macro, Silver, Quarantine và `schema.yml`.

### Logic chuẩn

- `1`, `2`, `3`, `4` giữ nguyên.
- `urgent`, `high`, `medium`, `low` map thành `1`, `2`, `3`, `4`.
- Giá trị khác trả NULL và vào quarantine.
- Trong Silver: chuẩn hóa -> lọc row lỗi -> `row_number()` -> lấy latest.
- Quarantine dùng cùng macro và giữ grain row CDC.
- Bật `contract.enforced: true`.
- Test `not_null` và `accepted_values [1,2,3,4]`, `quote: false`.

### Root cause

Source schema-evolve từ số sang nhãn hợp lệ, trong khi `try_cast` cũ vừa biến
nhãn thành NULL vừa chấp nhận số ngoài miền 1..4.

### Output

- Silver đủ 12.480 ticket, priority sạch.
- Quarantine đúng 312 row.
- 11/11 test pass, contract được enforce.

## 7. Bonus A - Compact Parquet (+5)

### Input

- `data/gold_events/`, `queries/dashboard.sql`, `tools/compact.py`.
- Baseline `make explain`: 5.000 file, 5.000.000 rows scanned.

### Logic chuẩn

1. Lưu baseline rows scanned, rows on disk, file count và result hash.
2. Compact bằng `COPY ... TO` sang dataset mới.
3. Partition theo `event_date` để engine loại partition trước khi mở file.
4. Sort `event_date`, `customer_name`, `event_time` để row-group statistics hữu
   ích cho customer và time range.
5. Chọn row group phù hợp thay vì để cả ngày thành một group quá lớn.
6. Query đọc dataset mới với `hive_partitioning = true`.
7. Dùng predicate sargable:

   ```sql
   event_time >= timestamp '2026-08-09 00:00:00'
   and event_time < timestamp '2026-08-10 00:00:00'
   ```

8. Giữ nguyên customer/date semantics và result hash.

### Root cause

Small-file problem, layout không khớp filter và `strftime(event_time, ...)`
không sargable khiến engine phải mở/quét toàn bộ file.

### Output

- Rows scanned: 5.000.000 -> 9.324, giảm 536,3x.
- File: 5.000 -> 14.
- Rows on disk: 130.683 -> 130.683.
- Hash giữ nguyên: `4379e4c5d9f3`.

## 8. Bonus B - Crash-safe consumer (+5)

### Input

- `ingest/consumer.py`, `ingest/log_client.py`, `tools/crash_test.py`.

### Logic chuẩn

1. Natural key `event_id` có `PRIMARY KEY`/`UNIQUE`.
2. Ghi bằng `ON CONFLICT (event_id) DO UPDATE` và cập nhật payload.
3. Thứ tự: poll -> write/upsert -> crash point -> commit offset.
4. Crash sau write/trước commit làm batch được replay.
5. Idempotent upsert làm replay không sinh duplicate.
6. Không gọi transport là exactly-once; đây là at-least-once kết hợp
   idempotent storage.
7. `DO UPDATE` phản ánh payload mới; `DO NOTHING` có thể giữ payload cũ.

### Root cause

Commit trước write tạo at-most-once: crash ở giữa làm offset dịch nhưng batch
chưa lưu, dẫn tới mất dữ liệu.

### Output

- 20.000 hàng / 20.000 `event_id` sau restart.
- Không mất, không trùng, `C == A`.
- `BÀI MỞ RỘNG B: ĐẠT`.

## 9. Report

Mỗi nhiệm vụ/bonus phải có Triệu chứng, Nguyên nhân, Cách khắc phục và Bằng
chứng. Root cause mô tả cơ chế, không chỉ kể thay đổi code.

- Nhiệm vụ 2 có P99 và lý do chọn lookback.
- Nhiệm vụ 3 giải thích Bronze/Silver và quarantine.
- Bonus A có scan/file/rows/hash trước-sau.
- Bonus B giải thích at-most-once, at-least-once, idempotency và upsert.
- Chỉ dùng số thực đo; dán output verify cuối.

## 10. Kiểm tra cuối và Definition of Done

```bash
make compact
make explain
make crash-test
make dbt-test
make verify
```

Sẵn sàng nộp 110/100 khi:

- `make verify` đạt 4/4, 4 checksum ổn định.
- Bonus A giảm scan >=10x, file giảm, hash không đổi.
- Bonus B crash-test đạt, không mất/trùng row.
- Report có đủ bằng chứng và root cause.
- Không có diff ở file cấm và `git diff --check` sạch.
- Không đóng gói `.venv/`, `warehouse.duckdb`, `data/`.
