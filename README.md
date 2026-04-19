# Gmail Web Gemini Bot

Bot Node.js theo phong cách class + worker giống style script mẫu của bạn, nhưng dùng **Playwright giả lập Gmail Web** thay vì Gmail API.

## Tính năng
- Không dùng Gmail API / OAuth client
- Mỗi tài khoản Gmail dùng **profile Chrome riêng**
- `npm run setup`: mở Gmail để bạn tự đăng nhập một lần
- `npm start`: lấy mail từ Gmail web, gửi Gemini chuẩn hóa JSON, ghi vào **một file Excel duy nhất**
- Dùng `worker_threads` để xử lý nhiều account song song
- Mỗi account có thể xử lý đồng thời nhiều mail
- Có màu log, intro `Script by dev Ha`, countdown, retry cơ bản

## Cài đặt
1. Cài **Node.js 18+**
2. Giải nén project
3. Trong thư mục project chạy:
   ```bash
   npm install
   npx playwright install chromium
   ```
4. Tạo file `.env` từ `.env.example`
5. Tạo file `accounts.json` từ `accounts.sample.json`
6. Tạo file `selected_accounts.txt` từ `selected_accounts.sample.txt`

## Cấu hình nhanh
### `.env`
```env
GEMINI_API_KEY=YOUR_GEMINI_API_KEY
GEMINI_MODEL=gemini-3-flash-preview
OUTPUT_EXCEL=./output/gmail_ai_output.xlsx
HEADLESS=false
MAX_ACCOUNT_THREADS=3
DEFAULT_MAX_EMAILS=50
DEFAULT_MAIL_CONCURRENCY=10
TIMEZONE=Asia/Ho_Chi_Minh
INTRO_NAME=dev Ha
```

### `accounts.json`
```json
[
  {
    "id": "acc_gmail_1",
    "email": "your-email@gmail.com",
    "enabled": true,
    "gmailUrl": "https://mail.google.com/mail/u/0/#inbox",
    "profileDir": "./profiles/acc_gmail_1",
    "maxEmails": 50,
    "mailConcurrency": 10,
    "onlyUnread": false,
    "query": "category:primary",
    "proxy": ""
  }
]
```

### `selected_accounts.txt`
```txt
acc_gmail_1
```

## Bước login Gmail lần đầu
Chạy:
```bash
npm run setup
```
Bot sẽ mở Chromium với profile riêng cho từng account đã chọn.

Việc bạn cần làm:
1. Đăng nhập Gmail thủ công trên cửa sổ mở ra
2. Chờ inbox hiện bình thường
3. Quay lại terminal và nhấn **Enter** để bot lưu session

Lần sau không cần login lại nếu profile vẫn còn.

## Chạy bot
```bash
npm start
```

## Excel đầu ra
Mặc định bot luôn append vào:
```txt
./output/gmail_ai_output.xlsx
```

Muốn tạo file Excel khác, đổi `OUTPUT_EXCEL` trong `.env`.

## Chống lấy trùng
Bot lưu trạng thái ở:
```txt
./state/<account-id>_processed.json
```

Nếu muốn quét lại từ đầu cho 1 account, xóa file state tương ứng.

## Cột Excel
- account_id
- email
- message_key
- gmail_url
- thread_url
- sender_name
- sender_email
- subject
- received_at_raw
- received_at_iso
- received_at_local
- body_text
- ai_json
- ai_status
- ai_confidence
- ai_error
- request_type
- customer_name
- phone
- email_extracted
- order_code
- amount
- currency
- note
- created_at

## Lưu ý quan trọng
- Gmail UI có thể đổi selector. Nếu Gmail đổi giao diện, sửa selector trong `index.js` là đủ.
- Không mở file Excel lúc bot đang ghi.
- Nếu profile lỗi, xóa thư mục `profiles/<account-id>` rồi `npm run setup` lại.
- Nếu Gmail hỏi xác minh thêm, login thủ công xong rồi mới nhấn Enter.

## Mẹo dùng ổn định
- Giữ `HEADLESS=false` khi test.
- Với nhiều account, nên bắt đầu `MAX_ACCOUNT_THREADS=2` hoặc `3`.
- `mailConcurrency=10` là đồng thời trong từng account. Nếu máy yếu, hạ xuống `3-5`.
