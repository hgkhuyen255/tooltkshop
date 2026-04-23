# Dùng Python 3.11 nhẹ và ổn định
FROM python:3.11-slim

# Tạo thư mục làm việc trong container
WORKDIR /app

# Copy file requirements và cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY . .

# Cloud Run sẽ tự đặt PORT trong biến môi trường, ta đọc nó khi chạy
CMD exec uvicorn main_2fa_full:app --host 0.0.0.0 --port ${PORT:-8080}
