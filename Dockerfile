FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code
COPY ./api/hot-service.py ./
COPY ./data-loading/muni_data_ingestor.py ./ 

# Expose port
EXPOSE 8000

# Run FastAPI app using uvicorn
CMD ["uvicorn", "hot-service:app", "--host", "0.0.0.0", "--port", "8000"]
