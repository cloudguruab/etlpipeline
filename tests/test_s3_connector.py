from xetra.common.s3_connector import S3BucketConnector
from moto import mock_s3 
from io import StringIO, BytesIO
import pandas as pd
# from testfixtures import LogCapture
# import logging
import boto3
import os

class TestConnectorObject:
    def setup_method(self):
        """
        instanciates mock connection,
        class arguments, and clear
        testing instances for our script
        """
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.s3_access_key = 'ACCESSKEY'
        self.s3_secret_key = 'SECRETKEY'
        self.s3_endpoint_url = 'https://s3.us-east-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={'LocationConstraint': ''})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        
    def teardown_method(self):
        self.mock_s3.stop()
        self.s3_endpoint_url = None
    
    def test_list_files_in_prefix(self):
        expected_prefix = 'prefix/'
        file_one = f'{expected_prefix}test1.csv'
        file_two = f'{expected_prefix}test2.csv'
        csv_content = """col1, col2 vala, valb"""
        
        self.s3_bucket.put_object(Body=csv_content, Key=file_one) 
        self.s3_bucket.put_object(Body=csv_content, Key=file_two) 
        list_result = self.s3_bucket_conn.list_files_in_prefix(expected_prefix)
        assert len(list_result) == 2
        assert file_one in list_result
        assert file_two in list_result
        
        self.s3_bucket.delete_objects(Delete={ #clean up bucket
            'Objects': [{
                'Key': file_one
            }, {
                'Key': file_two
            }]
        })
         
    def test_read_csv_to_df(self):
        expected_file = 'test.csv'
        exp_col1 = 'col1'
        exp_val1 = 'val1'
        exp_log = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{expected_file}'
        csv_obj = f'{exp_col1}\n{exp_val1}'
        self.s3_bucket.put_object(Body=csv_obj, Key=expected_file)
        
        # with LogCapture() as l: >>> debug
        #     logger = logging.getLogger()
        #     assert logger.debug(exp_log) in logger
        
        df_result = self.s3_bucket_conn.read_csv_to_df(expected_file)
        assert df_result.shape[0] == 1
        assert df_result.shape[1] == 1
        assert exp_val1 == df_result[exp_col1][0]
        self.s3_bucket.delete_objects(Delete={ #clean up bucket
            'Objects': [{
                'Key': expected_file
            }]
        })
        
    def test_write_data_to_s3(self):
        exp_return = None
        df_empty = pd.DataFrame()
        key = 'key.csv'
        file_format = 'csv'
        result = self.s3_bucket_conn.write_data_to_s3(df_empty, key, file_format)
        assert exp_return == result
        
    def test_write_data_to_s3_csv(self):
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.csv'
        file_format='csv'
        result = self.s3_bucket_conn.write_data_to_s3(df_exp, key_exp, file_format)
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        assert return_exp == result
        assert bool(df_exp.equals(df_result)) == True
        self.s3_bucket.delete_objects(Delete={ #clean up bucket
            'Objects': [{
                'Key': key_exp
            }]
        })
        
    def test_write_df_to_s3_parquet(self):
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.parquet'
        file_format = 'parquet'
        result = self.s3_bucket_conn.write_data_to_s3(df_exp, key_exp, file_format)
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        assert return_exp == result
        assert bool(df_exp.equals(df_result)) == True