import io
import pandas as pd
import yfinance as yf
import json
import gcsfs
import fastavro
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

def stock_data(GCS_KEY, quotes:str.upper, start_date, end_date):
    
    try:
        
        #ingest stock data from yfinance librabry   
        fetch_stock = yf.download(quotes, start=start_date, end=end_date, interval="1d")
        
        #check availability data
        if fetch_stock.empty:
            raise ValueError("No data available for the specified date range.")
        
        fetch_stock['Date'] = fetch_stock.index
        fetch_stock.reset_index(drop=True, inplace=True)
        
        fetch_stock = fetch_stock[['Date','Open', 'High', 'Low', 'Close']]
        
        stock_data_json = fetch_stock.to_json(orient='records')
        stock_data_json = json.loads(stock_data_json)
        
        #save to google cloud storage in new line delimiter json format
        gfs = gcsfs.GCSFileSystem(project='project_id', token=json.loads(GCS_KEY))
        
        with gfs.open(f'gs://bucket_name/api/yfinance/stock-data/{quotes}/{quotes}-{start_date}.json', 'w') as file_in:
            
            for iterate_stock_data in stock_data_json:
                
                date_column = iterate_stock_data.get('Date')
                converted_date = pd.to_datetime(date_column, unit='ms').date()
                converted_date_str = converted_date.strftime('%Y-%m-%d')
                open_column = str(iterate_stock_data.get('Open'))
                high_column = str(iterate_stock_data.get('High'))
                low_column = str(iterate_stock_data.get('Low'))
                close_column = str(iterate_stock_data.get('Close'))
                
                payload = {
                    'Quotes':quotes,
                    'Date':converted_date_str,
                    'Open':open_column,
                    'High':high_column,
                    'Low':low_column,
                    'Close':close_column
                }
                stock_data_payload = json.dumps(payload)
                file_in.write(stock_data_payload + "\n")
    except:
        pass

def create_avro(GCS_KEY, quotes, date):

    try:
        
        #define schema
        schema =  {
                    "name": "StockData",
                    "type" : "record",
                    "fields" : [
                        {"name": "Date", "type": "string"},
                        {"name": "Quotes", "type": "string"},
                        {"name": "Open", "type": "float"},
                        {"name": "High", "type": "float"},
                        {"name": "Low", "type": "float"},
                        {"name": "Close", "type": "float"}
                    ]
                }
        gfs = gcsfs.GCSFileSystem(project='project', token=json.loads(GCS_KEY))
        
        #load json file from google cloud storage    
        with gfs.open(f'gs://bucket_name/api/yfinance/stock-data/{quotes}/{quotes}-{date}.json', "rb") as raw_data:
            json_data = json.load(raw_data)   
            
            #convert to avro
            avro_data = {
                    "Date": json_data["Date"],
                    "Quotes": json_data["Quotes"],
                    "Open": float(json_data["Open"]),
                    "High": float(json_data["High"]),
                    "Low": float(json_data["Low"]),
                    "Close": float(json_data["Close"])
                }
            
            bytes_buffer = io.BytesIO()
            fastavro.writer(bytes_buffer, schema, [avro_data])
            
            avro_bytes = bytes_buffer.getvalue()
        
        #save avro to another bucket in google cloud storage
        with gfs.open(f'gs://bucket_name/api/yfinance/stock-data/{quotes}/{quotes}-{date}.avro', 'wb') as avro_output_file:            
            avro_output_file.write(avro_bytes)
        
        print("successfully create avro file")
    
    except:
        pass
        print("file or directory not exist")
        
def load_bigquery(GCS_KEY, table_id, quotes, date, write_disposition='WRITE_APPEND'):

    try:
        creds = service_account.Credentials.from_service_account_info(json.loads(GCS_KEY))

        bq_client = bigquery.Client(project="project_id", credentials=creds)
        destination_tabel = bq_client.dataset('dataset').table(table_id)

        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.AVRO,
            autodetect = True,
            write_disposition = write_disposition
        )
        
        uri = f'gs://bucket_name/api/yfinance/stock-data/{quotes}/{quotes}-{date}.avro'
            
        load_job = bq_client.load_table_from_uri(
    
            uri, 
            destination_tabel, 
            job_config=job_config
            
            )
        
        load_job.result()
        
        print(f"Data loaded")
        
    except:
        pass
        print('file or directory not exist')
    