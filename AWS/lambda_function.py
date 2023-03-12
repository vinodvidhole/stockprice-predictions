from yahoofinancials import YahooFinancials
from datetime import datetime, timedelta
import boto3
import csv
from io import StringIO

ls_ticker = 'GOOG'
yahoo_financials = YahooFinancials(ls_ticker)

today = datetime.today()
start_date = (today - timedelta(days=5)).strftime("%Y-%m-%d") #generate daily file for last 5 days 
end_date = today.strftime("%Y-%m-%d")

def upload_csv_s3(data_dictionary,s3_bucket_name,csv_file_name):
    data_dict = data_dictionary
    data_dict_keys = data_dictionary[0].keys()
    
    # creating a file buffer
    file_buff = StringIO()
    
    # writing csv data to file buffer
    writer = csv.DictWriter(file_buff, fieldnames=data_dict_keys)
    writer.writeheader()
    for data in data_dict:
        writer.writerow(data)
        
    # creating s3 client connection
    client = boto3.client('s3')
    
    # placing file to S3, file_buff.getvalue() is the CSV body for the file
    client.put_object(Body=file_buff.getvalue(), Bucket=s3_bucket_name, Key=csv_file_name)
    print('Done uploading to S3')
    
def lambda_handler(event, context):
    stock_prices = yahoo_financials.get_historical_price_data(start_date, end_date, 'daily')
    table_data = stock_prices[ls_ticker]['prices']
    for l in table_data:
        l.pop('date')
        l['ticker'] = ls_ticker
    table_rows = len(table_data)
    
    #create csv and upload in s3 bucket
    dt_string = datetime.now().strftime("%Y-%m-%d_%H%M")
    csv_file_name =  ls_ticker+'_' +dt_string +'.csv'
    upload_csv_s3(table_data,'stocksprice-prediction',csv_file_name)

    response = {
        "Rows": table_rows,
        "body": table_data
    }

    return response
    
if __name__ == "__main__":
    lambda_handler(None, None)