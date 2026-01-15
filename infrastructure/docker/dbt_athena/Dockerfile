FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash unzip git ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# dbt core + athena adapter
RUN pip install --no-cache-dir \
    dbt-core==1.8.2 \
    dbt-athena-community==1.8.2 \
    boto3

WORKDIR /app
CMD ["bash"]
