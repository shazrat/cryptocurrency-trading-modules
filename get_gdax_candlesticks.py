"""Author: Shiraz Hazrat (shiraz@shirazhazrat.com)

Collects minutely price/volume candlestick data (Open, High, Low, Close, Volume)
of all cryptocurrency markets (ex. 'BTC-USD') on GDAX exchange and stores into
AWS RDS-managed MySQL database and Google Firebase database.

Pulls 3 hours worth of data on each currency on every execution. 
Script can be called as frequently as you'd like, but MySQL row count operations
are time-consuming. I recommend running every 5 minutes.

On each execution, this script does the following:
    1. Get current list of markets on GDAX exchange.
    2. Determine if tables for each market exist in MySQL database.
    3. Create table if one doesn't exist (i.e. on first run, or when a new coin 
        is added to exchange.)
    4. Determine oldest entry in each table.
    5. Query exchange for a batch of data (3 hours worth) of minutely price data
        for each market.
    6. Save the batch to MySQL.
    7. Get total row count of each table.
    8. Update Google Firebase to be used for live dashboard metrics.

Requirements:
    $ pip install pymysql gdax
    RDS-managed MySQL server and database provisioned and online.
    
RDS Hostname, Credentials, DB name, Table name, and Firebase URL are all stored
in config.py.
"""

from __future__ import print_function

import gdax
import pymysql
import logging
import sys
import time
from datetime import datetime
from firebase import firebase
import config


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_firebase_app(URL):
    return firebase.FirebaseApplication(URL, None)

def get_initial_rowcount(fba, table):
    row_count = fba.get('crypto/rds_metrics/gdax/{}/'.format(table), "row_count")
    logging.info("Initial row count for table {}: {}".format(table, row_count))    
    return row_count

def save_row_count(fba, table, row_count):
    try:
        fba.put('crypto/rds_metrics/gdax/{}/'.format(table), "row_count", row_count)
    except Exception as e:
        logging.error("Count not save row count to Firebase. Error: {}".format(e))
        
def open_mysql_connection():
    try:
        connection = pymysql.connect(config.RDS_HOST, user=config.USER, passwd=config.PASSWORD, db=config.DB_NAME, connect_timeout=5)
    except Exception:
        logging.critical("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()
    return connection

def check_mysql_tables(connection, cursor, markets):
    for market in markets:
        query = 'show tables like "gdax_{}_candlesticks"'.format(market.lower())
        cursor.execute(query)
        result = cursor.fetchone()
        if result is None:
            logging.info("No table found for market {}. Creating one now...".format(market))
            query = 'CREATE TABLE `gdax_{}_candlesticks` (time bigint, low float,' \
                    ' high float, open float, close float, volume float, primary' \
                    ' key (time));'.format(market.lower())
            try:
                cursor.execute(query)
                connection.commit()
                logging.info("New table made successfully")
            except Exception:
                logging.error("Unable to create new MySQL table for market: {}".format(market))

def unix_to_iso(timestamp):
    return datetime.fromtimestamp(timestamp).isoformat()

def get_newest_oldest_entries(cursor, initial_row_count, table):
    if initial_row_count > 0:
        query = "SELECT MAX(time) FROM `{}`".format(table)
        cursor.execute(query)
        newest_entry = cursor.fetchone()[0]
        time_in_iso = unix_to_iso(newest_entry)
        logging.info("Newest entry in table {}: {}".format(table, time_in_iso))
        
        query = "SELECT MIN(time) FROM `{}`".format(table)
        cursor.execute(query)
        oldest_entry = cursor.fetchone()[0]
        time_in_iso = unix_to_iso(oldest_entry)
        logging.info("Oldest entry in table {}: {}".format(table, time_in_iso))
    
    else:
        newest_entry = round(time.time())
        oldest_entry = newest_entry
        logging.info("No entries exist. The newest timestamp will be: {}".format(newest_entry))

    return (newest_entry, oldest_entry)

def get_start_end_datetime(newest_entry, oldest_entry):
    end_time_unix = newest_entry + 10800 # 3 hours later
    forward_start_time = unix_to_iso(newest_entry)
    forward_end_time = unix_to_iso(end_time_unix)
    
    end_time_unix = oldest_entry - 10800 # 3 hours earlier
    backward_start_time = unix_to_iso(end_time_unix)
    backward_end_time = unix_to_iso(oldest_entry)
    
    return (forward_start_time, forward_end_time, backward_start_time, backward_end_time)

def get_public_client():
    logging.info("Initiating GDAX public client...")
    return gdax.PublicClient()

def get_gdax_markets(client):
    products = client.get_products()
    markets = []
    for i in products:
        markets.append(i['id'])
    return markets

def get_data_from_gdax(gdax_client, market, start, end, granularity=60):
    logging.info("Attempting to get data from GDAX...")
    try:
        data = gdax_client.get_product_historic_rates(
                    market,
                    start = start,
                    end = end, 
                    granularity = granularity)
    except:
        logging.error("Could not get data from GDAX")
    logging.info("{} entries retrieved from GDAX for market {}.".format(len(data), market))
    return data

def insert_rows(connection, cursor, table, data):
    for row in data:
        try:
            query = (
                    "REPLACE INTO `{0}` (time, low, high, open, close, volume)"
                    "VALUES ({1}, {2}, {3}, {4}, {5}, {6})".format(
                        table, row[0], row[1], row[2], row[3], 
                        row[4], row[5])
                    )
            cursor.execute(query)
            connection.commit()
        except Exception as e:
            logging.error("Error: Could not insert row. {}".format(e))
            
def get_total_rows(cursor, table):
    query = "SELECT COUNT(*) FROM `{}`".format(table)
    cursor.execute(query)
    total_rows = cursor.fetchone()[0]
    logging.info("{} total rows in table: {}".format(total_rows, table))
    return total_rows

def get_rows_added(initial_row_count, table, total_rows):
    rows_added = total_rows - initial_row_count
    logging.info("{} new row(s) added to table: {}.".format(rows_added, table))
    return rows_added

def update_firebase(fba, table, total_rows):
    fba.put('crypto/rds_metrics/gdax/{}/'.format(table), "row_count", total_rows)

def lambda_handler(event, context):
    connection = open_mysql_connection()
    cursor = connection.cursor()
    gdax_client = get_public_client()
    markets = get_gdax_markets(gdax_client)
    check_mysql_tables(connection, cursor, markets)
    fba = get_firebase_app(config.URL)
    for market in markets:
        table = 'gdax_{}_candlesticks'.format(market.lower())
        initial_row_count = get_initial_rowcount(fba, table)
        (newest_entry, oldest_entry) = get_newest_oldest_entries(cursor, initial_row_count, table)
        forward_start_time, forward_end_time, backward_start_time, backward_end_time = get_start_end_datetime(newest_entry, oldest_entry)
        forward_candlestick_data = get_data_from_gdax(gdax_client, market, forward_start_time, forward_end_time)
        insert_rows(connection, cursor, table, forward_candlestick_data)
        time.sleep(1)
        backward_candlestick_data = get_data_from_gdax(gdax_client, market, backward_start_time, backward_end_time)
        insert_rows(connection, cursor, table, backward_candlestick_data)
        total_rows = get_total_rows(cursor, table)
        rows_added = get_rows_added(initial_row_count, table, total_rows)
        update_firebase(fba, table, total_rows)
        time.sleep(1)
    cursor.close()
    connection.close()
    return "Success."