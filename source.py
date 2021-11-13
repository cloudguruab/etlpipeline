import boto3
import pandas as pd
from io import StringIO

s3 = boto3.resource('s3')
bucket = s3.Bucket('deutsche-boerse-xetra-pds')

bucket_obj = bucket.objects.filter(Prefix='2021-03-15')
bucket_obj2 = bucket.objects.filter(Prefix='2021-03-16')

objects = [obj for obj in bucket_obj] + [obj for obj in bucket_obj2]

#initialize object
csv_obj_initializer = bucket.Object(key=objects[0].key).get().get('Body').read().decode('utf-8')
data = StringIO(csv_obj_initializer)
df_init = pd.read_csv(data, delimiter=',')
df_all = pd.DataFrame(columns=df_init.columns)

#read data of objects
for obj in objects:
    csv_obj = bucket.Object(key=obj.key).get().get('Body').read().decode('utf-8')
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=',')
    df_all = df_all.append(df, ignore_index=True)
    # print(df_all) see whole data set
    
columns = ['ISIN', 'Date', 'Time', 'StartPrice',
           'MaxPrice', 'MinPrice', 'EndPrice', 
           'TradedVolume', 'NumberOfTrades']

data_points = df_all.loc[:, columns]
data_points.dropna(inplace=True)
# print(data_points.shape) print data count

#get opening price per isin and day

