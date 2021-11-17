import os
import boto3
import logging
from dot_env import load_dotenv

load_dotenv()

class S3BucketConnector:
       
    def __init__(self, access_key, secret_key, endpoint_url, bucket):
        """
        connector object for
        interacting with object stores
        :param access_key: access key for s3 
        :param secret_key: secret key for s3
        :param bucket: target bucket
        :param endpoint_url: endpoint for s3 target
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ.get('ACCESS_KEY'),
                                     aws_secret_access_key=os.environ.get('SECRET_KEY'))
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)
        
    def list_files_in_prefix(self):
        pass
    
    def read_csv_to_df(self):
        pass
    
    def write_data_to_s3(self):
        pass