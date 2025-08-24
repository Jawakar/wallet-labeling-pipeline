FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY export_hot_labels_to_end_table.py .
COPY utils_db.py .

CMD ["python", "export_hot_labels_to_end_table.py"]