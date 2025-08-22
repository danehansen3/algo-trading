FROM python:3.11-slim

# Set timezone to Eastern
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cron \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock* ./
COPY requirements.txt ./

# Install dependencies with uv (fallback to pip if no uv.lock)
RUN if [ -f uv.lock ]; then \
        uv sync --frozen; \
    else \
        uv pip install --system -r requirements.txt; \
    fi

# Copy application code
COPY data_processes/ ./data_processes/
COPY run_scanner.py .

# Create logs directory
RUN mkdir -p /app/logs

# Add cron job - runs at 9:20 AM Eastern (5 minutes before market open)
RUN echo "20 9 * * 1-5 cd /app && /usr/local/bin/python run_scanner.py >> /app/logs/scanner.log 2>&1" > /etc/cron.d/premarket-scan

# Set cron permissions
RUN chmod 0644 /etc/cron.d/premarket-scan
RUN crontab /etc/cron.d/premarket-scan

# Start cron and keep container running
CMD service cron start && tail -f /dev/null
