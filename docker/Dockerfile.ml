FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash unzip ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# ML dependencies
RUN pip install --no-cache-dir \
    awswrangler==3.9.1 \
    lightgbm==4.5.0 \
    scikit-learn==1.5.2 \
    pandas==2.2.3 \
    joblib==1.4.2 \
    boto3 \
    pyarrow


WORKDIR /app
CMD ["bash"]