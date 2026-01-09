





import boto3
import logging
from botocore.exceptions import ClientError
from pathlib import Path
from datetime import datetime


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
            

    def upload_object(self, localfile : Path, table_name : str) -> str:
        """ upload a dimension table (one csv file from local fiels) to s3://bucket/raw/{table}/{table}.csv """
        

        key = f"raw/{table_name}/{table_name}.csv"

        if self._object_exists(key):
            logger.info(f"dimension exist, overwriting s3://{self.bucket_name}/{key}")

        self.s3.upload_file(Filename = localfile, Bucket = self.bucket_name, Key= key,
                             ExtraArgs={"Metadata": {"upload_timestamp": datetime.now().isoformat(),"source_file": localfile.name,"table_type": "dimension",}}
        )

        file_size = localfile.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Uploaded {table_name} ({file_size:.2f} MB)")
        return key
        




def main():
    # ingestion = S3DataIngestion("retailops-data-lake-eu-west-2")
    # ingestion._check_bucket_access()
    # logger.info(ingestion._object_exists('raw/stores/stores.csv'))

    # # logger.info(ingestion.upload_object(Path("../data/synthetic/stores.csv") ,"stores"))



if __name__ == "__main__":
    main()
