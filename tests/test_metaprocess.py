import boto3
import os
import pandas as pd
from datetime import datetime, timedelta
from moto import mock_s3
from xetra.common.s3_connector import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat

class TestMetaProcessMethods:
    def setup_method(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_KEY'
        self.s3_endpoint_url = 'https://s3.us-east-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocationConstraint': ''
                              })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        self.s3_bucket_meta = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        self.dates = [(datetime.today().date() - timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]
        
    def teardown_method(self):
        self.mock_s3.stop()
        
    def test_update_meta_file_no_meta_file(self):
        date_list = ['2021-04-16', '2021-04-17']
        processed_date_list = [datetime.today().date()]*2
        meta_key = 'meta.csv'
        MetaProcess.update_meta_file(date_list, meta_key, self.s3_bucket_meta)
        
        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        buffer = StringIO(data)
        df_meta_result = pd.read_csv(buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        processed_date_list_result = list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value])\
                .dt.date
        )
        assert date_list == date_list_result
        assert processed_date_list == processed_date_list_result
        self.s3_bucket.delete_objects(Delete={'Key':meta_key})
        
    def test_update_meta_file(self):
        pass
    
    def test_return_data_lst(self):
        pass 
    