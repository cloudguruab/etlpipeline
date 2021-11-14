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

df_all.loc[:, columns]
df_all.dropna(inplace=True)

#get data count
# print(data_points.shape)

#get opening price per isin and day
df_all['opening_price'] = df_all.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')

#get closing price per ticker(ISIN) and day
df_all['closing_price'] = df_all.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('last')

#aggregate data into new format
df_all = df_all.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'),
                                                              closing_price_eur=('closing_price', 'min'),
                                                              minimum_price_eur=('MinPrice', 'min'),
                                                              maximum_price_eur=('MaxPrice', 'max'),
                                                              daily_traded_volume=('TradedVolume', 'sum'))

#percent change previous closing 
df_all['prev_closing_price'] = df_all.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
df_all['change_prev_closing_%'] = (df_all['closing_price_eur'] - df_all['prev_closing_price']) / df_all['prev_closing_price'] * 100
df_all.drop(column=['prev_closing_price'], inplace=True) #no longer need prev_closing_price
df_all = df_all.round(decimals=2)

print(df_all)