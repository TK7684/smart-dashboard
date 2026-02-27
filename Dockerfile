FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY data_pipeline.py .
COPY dashboard.py .
COPY utils.py .

# Copy analytics modules
COPY analytics /app/analytics

# Copy pages directory for multipage app
COPY pages /app/pages

# Copy Streamlit config for light theme
COPY .streamlit /app/.streamlit

# Create directories for data
RUN mkdir -p /app/processed_data /app/Shopee_orders /app/Shopee_Ad /app/Shopee_Live /app/Shopee_Video

# Expose Streamlit port
EXPOSE 8501

# Set Streamlit config
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Run dashboard
CMD ["python", "-m", "streamlit", "run", "dashboard.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
