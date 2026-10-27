FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

EXPOSE 8000
# 0.0.0.0 permite acceder desde cualquier dispositivo de la red (celular, tablet, otro PC)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
