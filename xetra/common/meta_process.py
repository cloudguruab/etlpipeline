import pandas as pd
import collections
from datetime import datetime
from .constants import MetaProcessFormat
from .exceptions import WrongMetaFileException
from .s3_connector import S3BucketConnector

class MetaProcess:
    @staticmethod
    def update_meta_file(extract_data_lst: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        df_new = pd.DataFrame(columns=[
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value
        ])
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_data_lst
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = \
            datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        
        try:
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            df_all = df_new
            
        s3_bucket_meta.write_data_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True
        
        
    @staticmethod
    def return_data_lst(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        start = datetime.strptime(first_date,
                                  MetaProcessFormat.META_DATE_FORMAT.value)\
                                      .date() - timedelta(days=1)
            
        today = datetime.today().date()
        try: 
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key)
            dates = [start+timedelta(days=x) for x in range(0, (today-start).days +1)]
            src_dates = set(pd.to_datetime(
                df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]
            ).dt.date)
            dates_missing = set(dates[1:]) - src_dates
            if dates_missing: 
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                return_min_date = (min_date + timedelta(days=1))\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = [
                    date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                        for date in dates if date >= min_date
                ]
            else: 
                return_dates = []
                return_min_date = datetime(2200, 1, 1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            return_min_date = first_date
            return_dates = [
                (start + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                    for x in range(0, (today-start).days +1)
            ]
        
        return return_min_date, return_dates