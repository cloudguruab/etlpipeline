import os
import boto3
import logging
import pandas as pd
from .constants import FileTypes
from io import StringIO, BytesIO
from dotenv import load_dotenv
from .exceptions import WrongFormatException

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
        
    def list_files_in_prefix(self, prefix: str):
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files
    
    def read_csv_to_df(self, key: str, encoding: str='utf-8', sep: str=','):
        self._logger.info(f'Reading file {self.endpoint_url} {self._bucket.name} {key}')
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)
        return data_frame
    
    def write_data_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        if data_frame.empty:
            self._logger.info("no write access, file doesnt exists")
            return None
        
        if file_format == FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        
        if file_format == FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        
        self._logger.info('This file format is not supported', file_format)
        raise WrongFormatException
    
    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        self._logger.info(f"Writing file to {self.endpoint_url}, {self._bucket.name}, {key}")
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True