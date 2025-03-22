import pandas as pd
import chardet as ch 
from pathlib import Path
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 

#load variables
load_dotenv(dotenv_path='./venv/.env')
username=os.getenv('USERNAME')
host= os.getenv('HOST')
password=os.getenv('PASSWORD')
port=os.getenv('PORT')

def extract_data(filename):
    # get path to CSV file
    curr_dir=Path.cwd()
    file_path=curr_dir/'CSV'/filename
    # Detect the encoding of the CSV file
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        encoding_result = ch.detect(raw_data)
        file_encoding = encoding_result['encoding']
        print(file_encoding)
    try:
        # Read csv file
        data=pd.read_csv(file_path,sep=',', encoding=file_encoding, quotechar='"')
        return data
    except Exception as e :
        print('extracting data error'+str(e))
    


categories_dataset=extract_data('categories.csv')
countries_dataset=extract_data('countries.csv')
customers_dataset=extract_data('customers.csv')
employees_dataset=extract_data('employees.csv')
products_dataset=extract_data('products.csv')
sales_dataset=extract_data('sales.csv')
cities_dataset=extract_data('cities.csv')

def transform_data_employees():
    df=pd.DataFrame(employees_dataset)
    # convert BirthDate and HireDate to date format
    df['BirthDate']=pd.to_datetime(df['BirthDate']).dt.date
    df['HireDate']=pd.to_datetime(df['HireDate']).dt.date
    # drop column MiddleInitial
    df=df.drop('MiddleInitial',axis=1,inplace=False)
    return df
    
def transform_data_products():
     df=pd.DataFrame(products_dataset)
     #changing ModifyDate column to date format
     df['ModifyDate']=pd.to_datetime(df['ModifyDate']).dt.date
     # replace ',' in productName
     df['ProductName']=df['ProductName'].replace(",","-")
     #convert CategoryID column to int 
     df['CategoryID']=df['CategoryID'].fillna(0).astype(int)
     #replace null value with N/A
     df['Class']=df['Class'].fillna('N/A')
     # replace null value with unknown
     df['Resistant']=df['Resistant'].fillna('Unknown')
     df['IsAllergic']=df['IsAllergic'].fillna('Unknown')
     # convert VitalityDays column from float to int type
     df['VitalityDays'] = df['VitalityDays'].replace([np.nan, np.inf, -np.inf], 0).astype(int)
     #convert null values to zero in Price column
     df['Price']=df['Price'].fillna(0)
     return df 


def transform_data_sales():
    df=pd.DataFrame(sales_dataset)
    # remove duplicates
    df= df.drop_duplicates()
    # drop rows that column salesdate doesnt have value
    df=df.dropna(subset=['SalesDate'])
    # convert salesdate column to date format
    df['SalesDate']=pd.to_datetime(df['SalesDate']).dt.date
    # convert column quantity from float to int
    df['Quantity']=df['Quantity'].astype(int)
    return df

def transform_data_customers():
    # drop column MiddleInitial
    df=pd.DataFrame(customers_dataset)
    df=df.drop('MiddleInitial',axis=1,inplace=False)
    return df
    

def load_data(table_name,df):
    try:
        engine=create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/ETL')
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Rows imported from {table_name} is {len(df)}')
    except Exception as e:
        print('data load error'+str(e))


df=pd.DataFrame(categories_dataset)
load_data('Categories',df)
df=pd.DataFrame(cities_dataset)
load_data('Cities',df)
df=transform_data_customers()
load_data('Customers',df)
df=transform_data_employees()
load_data('Employees',df)
df=transform_data_products()
load_data('Products',df)
df=transform_data_sales()
load_data('Sales',df)
df=pd.DataFrame(countries_dataset)
load_data('Countries',df)


