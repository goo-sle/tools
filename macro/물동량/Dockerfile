FROM mcr.microsoft.com/playwright/python:v1.50.0-jammy

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

EXPOSE 8080
CMD ["python", "server.py"]
