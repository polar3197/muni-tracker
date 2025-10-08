FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code
COPY ./ /app/

ENV PYTHONPATH=/app

# Run backup dummy file 
CMD ["python", "hello.py"]
