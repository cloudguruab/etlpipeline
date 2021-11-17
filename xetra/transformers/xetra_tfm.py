from typing import NamedTuple
from xetra.common.s3_connector import S3BucketConnector

class XetraSourceConfig(NamedTuple):
    """
    Source configuration for data
    
    src_first_extract_date: determine the date for source
    src_columns: column names for source
    src_col_date: col name for source
    src_col_isin: col name for source
    src_col_time: col name for source
    src_col_start_price: col name for start price
    src_col_min_price: col name for min price
    src_col_max_price: col name for max price
    src_col_traded_vol: col name for traded volume 
    """
    src_first_extract_date:str
    src_columns:list
    src_col_date:str
    src_col_isin:str
    src_col_time:str
    src_col_start_price:str
    src_col_min_price:str
    src_col_max_price:str
    src_col_traded_vol:str
    
class XetraTargetConfig(NamedTuple):
    """
    Target configuration for data
    
    trg_col_isin: col name for isin in target
    trg_col_date: col name for date in target
    trg_col_op_price: col name for opening price
    trg_col_clos_price: col name for closing price
    trg_col_min_price: col name for min price
    trg_col_max_price: col name for max price
    trg_col_daily_trade_vol: col name for daily traded volume
    trg_col_ch_pre_close: col name for change in previous day closing price
    trg_key: basic key of target file
    trg_key_data_format: date format for target file key
    trg_format: format for target file
    """
    trg_col_isin:str
    trg_col_date:str
    trg_col_op_price:str
    trg_col_clos_price:str
    trg_col_min_price:str
    trg_col_max_price:str
    trg_col_daily_trade_vol:str
    trg_col_ch_pre_close:str
    trg_key:str
    trg_key_data_format:str
    trg_format:str
    
class XetraETL:
  
    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector, 
                 meta_key: str, src_args: XetraSourceConfig,
                 trg_args: XetraTargetConfig):
        """
        reads xetra data, transform, 
        and writes to target
        
        :param s3_bucket_src: connection to source bucket
        :param s3_bucket_trg: connection to target bucket
        :param meta_key: used for self.meta_key => key for meta file
        :param src_args: NamedTuple class with source configuration data
        :param trg_args: NamedTuple class with target configuration data
        """
        
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_data = None
        self.extract_data_lst = None
        self.meta_update_lst = None
        
    def extract(self):
        pass
    
    def transform(self):
        pass
    
    def load(self):
        pass
    
    def report(self):
        pass
         