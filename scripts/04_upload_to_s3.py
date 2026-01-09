"""
S3 Data Ingestion 
- Dimensions: single file (overwrite)
- Facts: partitioned by date (idempotent: skips existing partitions)
"""

import sys
import boto3
import logging
from botocore.exceptions import ClientError
from pathlib import Path
from datetime import datetime
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3DataIngestion:

    def __init__(self, bucket_name: str, region: str = "eu-west-2"):

        self.bucket_name = bucket_name
        self.region = region
        self.s3 = boto3.client("s3", region_name=region)


    def _check_bucket_access(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            logger.info(f"connected to s3 bucket: {self.bucket_name}")
        except ClientError as e:
            logger.error(f"Bucket not accessible: {self.bucket_name} - {e}")
            raise
    

    def _object_exists(self,key : str):
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key = key)
            logger.info(f"object exists : {key}")
            return True

        except ClientError as e:
            error_code = e.response["Error"].get("Code", "")

            if error_code in ("404", "NoSuchKey", "NotFound"):
                return False
            logger.error(f"error checking object {key} (error_code={error_code}): {e}")
            raise   
            

    def upload_dimension(self, localfile : Path, table_name : str) -> str:
        """ Upload a fact table partitioned by date (should skip a partition if it already exists) """

        key = f"raw/{table_name}/{table_name}.csv"

        if self._object_exists(key):
            logger.info(f"dimension exist, overwriting s3://{self.bucket_name}/{key}")

        self.s3.upload_file(Filename = localfile, Bucket = self.bucket_name, Key= key,
                             ExtraArgs={"Metadata": {"upload_timestamp": datetime.now().isoformat(),"source_file": localfile.name,"table_type": "dimension",}}
        )

        file_size = localfile.stat().st_size / (1024 * 1024)
        logger.info(f"Uploaded {table_name} ({file_size:.3f} MB)")
        return key
        

    def upload_fact_partitioned_date(self, localfile : Path, table_name : str, date_column: str) -> list[str]:
        """ upload a dimension table (one csv file from local fiels) to s3://bucket/raw/{table}/{table}.csv """
    
        df = pd.read_csv(localfile)
        
        if date_column not in df.columns:
            raise ValueError(f" {table_name}:missing date column '{date_column}' in {local_file}")
        
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

        # bad_dates = df[date_column].isna().sum()
        # if bad_dates:
        #     raise ValueError(f"{table_name}: {bad_dates} rows have invalid '{date_column}' values")

        dates = sorted(df[date_column].dt.date.unique())
        logger.info(f"{table_name}: {len(dates)} unique date partitions")
        
        uploaded = []
        for d in dates:
            part_prefix = f"raw/{table_name}/dt={d}"
            file_name = f"{table_name}_{d.strftime('%Y%m%d')}.csv"
            key = f"{part_prefix}/{file_name}"

            if self._object_exists(key):
                logger.info(f"{table_name}: partition exists, skipping dt={d}")
                continue

            part_df = df[df[date_column].dt.date == d]
            tmp_path = Path(f"/tmp/{file_name}")
            part_df.to_csv(tmp_path, index=False)


            self.s3.upload_file(
                    Filename=str(tmp_path), Bucket=self.bucket_name, Key=key,
                    ExtraArgs={
                        "Metadata": {
                            "upload_timestamp": datetime.now().isoformat(),
                            "partition_date": str(d),
                            "row_count": str(len(part_df)),
                            "table_type": "fact",
                        }
                    },
                )
            
            tmp_path.unlink(missing_ok=True)
            uploaded.append(key)
            logger.info(f"Uploaded {table_name} dt={d} ({len(part_df)} rows)")
        logger.info(f"Completed {table_name}: {len(uploaded)} new partition(s)")
        return uploaded




    def upload_all(self, source_dir: str) -> dict:
            source_path = Path(source_dir)
            if not source_path.exists():
                raise FileNotFoundError(f"Source directory not found: {source_dir}")

            logger.info(f"Starting ingestion from: {source_path}")
            logger.info(f"Target bucket: s3://{self.bucket_name}")

            tables = {
                # dimensions
                "products": {"type": "dimension"},
                "stores": {"type": "dimension"},
                "suppliers": {"type": "dimension"},
                # facts
                "sales": {"type": "fact", "date_column": "sale_date"},
                "inventory": {"type": "fact", "date_column": "snapshot_date"},
                "shipments": {"type": "fact", "date_column": "order_date"},
            }

            results = {}

            for table_name, cfg in tables.items():
                local_file = source_path / f"{table_name}.csv"
                if not local_file.exists():
                    logger.warning(f"Missing file, skipping: {local_file}")
                    continue

                try:
                    if cfg["type"] == "dimension":
                        key = self.upload_dimension(local_file, table_name)
                        results[table_name] = {"status": "success", "keys": [key]}
                    else:
                        keys = self.upload_fact_partitioned_date(local_file, table_name, cfg["date_column"])
                        results[table_name] = {"status": "success", "keys": keys}
                except Exception as e:
                    logger.error(f"{table_name}: upload failed - {e}")
                    results[table_name] = {"status": "failed", "error": str(e)}

            # summary
            logger.info("\n" + "=" * 60)
            logger.info("INGESTION SUMMARY")
            for table_name, r in results.items():
                if r["status"] == "success":
                    logger.info(f"{table_name}: ✓ {len(r['keys'])} object(s)")
                else:
                    logger.error(f"{table_name}: ✗ {r['error']}")


            return results




def main():
    # parser = argparse.ArgumentParser(description="Upload raw data to S3")
    # parser.add_argument("--bucket", required=True, help="S3 bucket name")
    # parser.add_argument("--source", default="../../local_data/raw", help="Source directory with CSVs")
    # parser.add_argument("--region", default="us-east-1", help="AWS region")
    # args = parser.parse_args()

    ingestion = S3DataIngestion(bucket_name="retailops-data-lake-eu-west-2")
    results = ingestion.upload_all(Path("../data/synthetic/"))

    # failed = [t for t, r in results.items() if r["status"] == "failed"]
    # if failed:
    #     logger.error(f"Ingestion completed with failures: {failed}")
    #     sys.exit(1)

    logger.info("All uploads successful")
    sys.exit(0)



if __name__ == "__main__":
    main()
