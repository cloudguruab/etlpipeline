# ETL Pipline 
The Deutsche Börse Public Dataset (PDS) project makes near-time data derived from Deutsche Börse's trading systems available to the public for free. This data is provided on a minute-by-minute basis and aggregated from the Xetra engines, which comprise a variety of equities, funds and derivative securities. The PDS contains details for on a per security level, detailing trading activity by minute including the high, low, first and last prices within the time period.

This project is uses the data from the PDS project and feeds this data into an s3 bucket, transforms this data and loads it into object store ready for OLAP based functions. I have built this pipeline to be compatible with the AWS cloud environment, Python, Pandas, and Boto3(aws sdk). 
### Setting up Virtual Enviornment

### Test Enviornment

### Code snippet
Here is an example of data transformations using pandas dataframes. I wanted
to transform our data frame by taking the previous closing price to calculate the
percent change in closing price overall for german equities. 

To do this I: 
- Create a new column 
- Use the column to perform simple arithmetic
- Drop the column since we no longer need it
- Round our data in our dataframe

```
df_all['prev_closing_price'] = df_all.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
df_all['change_prev_closing_%'] = (df_all['closing_price_eur'] - df_all['prev_closing_price']) / df_all['prev_closing_price'] * 100
df_all.drop(column=['prev_closing_price'], inplace=True)
df_all = df_all.round(decimals=2)
```

### Pulling data from Xetra Engine

- Accessing s3 bucket for source data 
```
aws s3 ls deutsche-boerse-xetra-pds/<date you want to get> --recursive --no-sign-request
```

### Docs
Walk through on accessing data and high level of architecture can be found in docs.