import pandas as pd
import collections
from .exceptions import WrongMetaFileException
from .s3_connector import S3BucketConnector

class MetaProcess:
    @staticmethod
    def update_meta_file(extract_data_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        df_new = pd.Dataframe(columns=[
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value
        ])
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_data_lst
        df_new[MeaProcessFormat.META_PROCESS_COL.value] = \
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
    def return_data_lst():
        pass