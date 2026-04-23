FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash unzip ca-certificates libgomp1 \
  && rm -rf /var/lib/apt/lists/*

# ML dependencies
RUN pip install --no-cache-dir \
    awswrangler==3.9.1 \
    lightgbm==4.5.0 \
    scikit-learn==1.5.2 \
    pandas==2.2.3 \
    joblib==1.4.2 \
    boto3 \
    pyarrow \
    python-dotenv==1.0.1 \
    optuna==3.6.1 \
    shap==0.45.1 \
    scipy==1.13.1

WORKDIR /app
CMD ["bash"]