import requests
import re
import os
import pandas as pd
import pandasql as pdsql
import logging
from datetime import datetime, timedelta, date
from airflow.exceptions import AirflowException
from zipfile import ZipFile
from dateutil.relativedelta import relativedelta




def __get_file(url):

    resp = requests.get(url, allow_redirects=True)

    logging.info("Start Downloading File")
    if resp.status_code == 200:

        directories = 'result_2/'

        if not os.path.exists(directories):
            os.makedirs(directories) 

        logging.info("Download File Success")
        open('result_2/dataset.zip', 'wb').write(resp.content)

        # unzip downloaded file
        with ZipFile('result_2/dataset.zip', 'r') as zipObj:
            # Extract all the contents of zip file
            zipObj.extractall('result_2/')

def __write_to_csv(df, target_directory):

    if not os.path.exists(target_directory):
        os.makedirs(target_directory) 

    filename = 'result_2.csv'
    logging.info("Process: Will write CSV file to directory: {}".format(target_directory))
    df.to_csv(path_or_buf=target_directory + '/' + filename, index=False)
    logging.info("Process: Write CSV file to directory: {} Success!".format(target_directory))

def run(**kwargs):
    # Print input parameters
    execution_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    logging.info("Execution datetime={}".format(execution_date))
    logging.info("Params={}".format(kwargs["params"]))

    # Get value from argument
    url = kwargs["params"]["url"]

    if not os.path.exists('result_2/dataset/'):
        # get dataset
        __get_file(url)

    root= "result_2/dataset/"
    csvs= {}  #container for the various csvs contained in the directory
    dfs = {}  #container for temporary dataframes

    # collect csv filenames and paths 
    for dirpath, dirnames, filenames in os.walk(root):
        for file in filenames:
            csvs.update({'{}'.format(file):'{}'.format(dirpath + '/' + file)})

    # store each dataframe in the dict
    for filename, path in csvs.items():
        filename = os.path.splitext(filename)[0]
        dfs.update({'{}'.format(filename): pd.read_csv(path)})


    # Replace customer_code column to uppercase
    dfs['customers']['customer_code'] = dfs['customers']['customer_code'].str.upper() 

    # Replace name column to uppercase
    dfs['customers']['name'] = dfs['customers']['name'].str.upper() 

    # Replace Non Alphabetical characters to whitespace ' '
    dfs['customers']['name'] = dfs['customers']['name'].replace(to_replace ='[^A-Z]', value = ' ', regex = True)

    # Replace invalid phone number to 'UNKNOWN'
    dfs['customers'].loc[~dfs['customers']['phone_number'].str.contains('^(0|\+)[0-9]*$', regex = True), 'phone_number'] = 'UNKNOWN'



    df_customer = dfs['customers']
    logging.info("Dataframe Customer: \n{}".format(df_customer))
    df_products = dfs['products']
    logging.info("Dataframe Products: \n{}".format(df_products))
    df_orders = dfs['orders']
    logging.info("Dataframe Orders: \n{}".format(df_orders))
    df_deliveries = dfs['deliveries']
    logging.info("Dataframe Deliveries: \n{}".format(df_deliveries))

    result = """
            WITH customer_total_price AS (
            SELECT do.customer_id AS customer_id
                , SUM(CASE
                        WHEN do.is_cancelled == 1 THEN 0
                        ELSE dp.price
                    END) AS total_price
            FROM df_orders AS do
            LEFT JOIN df_products AS dp ON dp.id == do.product_id
            GROUP BY 1
            ), orders AS (
            SELECT do.customer_id AS customer_id
                , count(*) AS total_order
                , SUM(CASE
                        WHEN do.is_cancelled == 1 THEN 0
                        ELSE 1
                    END) AS order_success
                , SUM(CASE
                        WHEN do.is_cancelled == 1 THEN 1
                        ELSE 0
                    END) AS order_cancelled
                , SUM(CASE
                        WHEN do.is_cancelled == 1 THEN 0
                        ELSE dp.price
                    END) AS total_price
            FROM df_orders as do
            LEFT JOIN df_products AS dp ON dp.id == do.product_id
            WHERE order_date >= '{last_3_months}'
            GROUP BY 1
            ), loyalty_level as (
            SELECT customer_id
                , total_order
                , order_success
                , order_cancelled
                , CASE
                    WHEN total_order >= 10
                        AND order_cancelled == 0 THEN 'PLATINUM'
                    WHEN total_order >= 10
                        AND order_cancelled <=3 THEN 'GOLD'
                    WHEN total_order >= 7
                        AND order_cancelled <= 1 THEN 'GOLD'
                    WHEN total_order >= 7
                        AND order_cancelled BETWEEN 2 AND 3 THEN 'SILVER'
                    WHEN total_order >= 4
                        AND order_cancelled == 0 THEN 'SILVER'
                    ELSE 'BRONZE'
                END as loyalty_level
            FROM orders
            ), is_active as (
            SELECT customer_id
                , CASE
                    WHEN SUM(CASE
                                WHEN order_date >= '{is_active_date}' THEN 1
                                ELSE 0
                                end) > 1 then 'True'
                    ELSE 'False'
                END AS is_active
            FROM df_orders
            GROUP BY 1
            ), customers AS (
            SELECT id
                , customer_code
                , name
                , phone_number
            FROM df_customer
            )
            SELECT c.customer_code   AS customer_code
                 , c.name            AS name
                 , c.phone_number    AS phone_number
                 , ia.is_active      AS is_active
                 , CASE
                    WHEN ll.loyalty_level IS NULL THEN 'BRONZE'
                    ELSE ll.loyalty_level
                 END                 AS loyalty_level
                 , ctp.total_price   AS total_price
            FROM customers AS c
            LEFT JOIN is_active AS ia ON ia.customer_id = c.id
            LEFT JOIN loyalty_level AS ll ON ll.customer_id = c.id
            LEFT JOIN orders AS o ON o.customer_id = c.id
            LEFT JOIN customer_total_price AS ctp on ctp.customer_id = c.id
            """.format(is_active_date = date.today() - relativedelta(months=+6),
                    last_3_months = date.today() - relativedelta(months=+3))

    df_result = pdsql.sqldf(result, locals())

    logging.info("Dataframe Result: \n{}".format(df_result))

    # write result in csv format
    __write_to_csv(df_result, target_directory='result_2/csv_output')

    