import pandas as pd 
import numpy as np
import requests 
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_name ='Largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
log_file = 'code_log.txt'
csv_path = 'exchange_rate.csv'
output_path = './Largest_banks_data.csv'
sql_connection = sqlite3.connect('Banks.db')


def log_progress(message):
    timestamp_format = '%h-%y-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file , 'a') as f:
        f.write(timestamp + ' , ' + message + '\n')




def extract(url, table_attribs):
    df = pd.DataFrame(columns = table_attribs)
    page = requests.get(url).text
    data = BeautifulSoup(page , 'html.parser')
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 :
            data_dict = {'Name': col[1].contents[2] , 'MC_USD_Billion': col[2].contents[0]}
            df1 = pd.DataFrame(data_dict , index = [0])
            df1['MC_USD_Billion'] = [float(df1['MC_USD_Billion'].str.replace("\n" , ""))]
            df = pd.concat([df , df1] , ignore_index=True)
    return df



def transform(df, csv_path):
    dataframe = pd.read_csv(csv_path)
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'] , 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'] , 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'] , 2) for x in df['MC_USD_Billion']]
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path)



def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name , sql_connection , if_exists='replace' , index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement , sql_connection)
    print(query_output)



log_progress("ETL started")

log_progress("Extraction phase started")
extracted_data = extract(url, table_attribs)
log_progress("Extraction phase ended")

log_progress("Transformation phase started")
transformed_data = transform(extracted_data , csv_path)
log_progress("Transformation phase ended")

log_progress("Load phase started")
load_to_csv(transformed_data , output_path)
log_progress('Data saved to CSV file')


load_to_db(transformed_data , sql_connection , table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement_1 = 'SELECT * FROM Largest_banks'
run_query(query_statement_1, sql_connection)

query_statement_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement_2, sql_connection)

query_statement_3 = 'SELECT Name from Largest_banks LIMIT 5'
run_query(query_statement_3, sql_connection)

log_progress("Process Completed")

sql_connection.close()