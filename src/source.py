import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timedelta

grab_by_date = '2021-11-05'
arg_date_dt = datetime.strptime(grab_by_date, '%Y-%m-%d').date() - timedelta(days=1)

s3 = boto3.resource('s3')
bucket = s3.Bucket('deutsche-boerse-xetra-pds')

objects = [obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], '%Y-%m-%d').date() >= arg_date_dt]

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
df_all.drop(columns=['prev_closing_price'], inplace=True) #no longer need prev_closing_price
df_all = df_all.round(decimals=2)
# df_all = df_all[df_all.Date >= arg_date_dt]

#write to s3
buffer = BytesIO()
save_key = 'daily_report_' + datetime.today().strftime('%Y%m%d_%H%M%S') + '.parquet'
df_all.to_parquet(buffer, index=False)
target = s3.Bucket('xetra-data-1234')
target.put_object(Body=buffer.getvalue(), Key=save_key)

#reading from the target bucket