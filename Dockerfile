FROM python:3.12-slim

WORKDIR /app

# dbt deps needs git
RUN apt-get update && apt-get install -y --no-install-recommends git \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["bash"]
