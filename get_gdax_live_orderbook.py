""" Uses websockets to pull realtime orderbook data from GDAX exchange.
    Author: Shiraz Hazrat (shiraz@shirazhazrat.com)

    Orderbook information is dense. I reduce dimensions by grouping orders based
    on percent ranges from current price, and discarding any orders beyond that.

    i.e. If BTC price is at $10,000, I don't care for buy orders at $5,000, or 
    sell orders at $15,000.

    Saves results to MySQL database provisioned in AWS RDS.
"""

from websocket import create_connection
import pymysql
import json
import numpy as np
import logging
import sys
import time


def openMySQLConnection():
    try:
        connection = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    except Exception:
        logging.critical("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()
    return connection


def get_orderbook():
    ws = create_connection("wss://ws-feed.gdax.com")
    request = json.dumps({
                         "type": "subscribe",
                         "product_ids": ["BTC-USD"],
                         "channels": ["level2"]
                         })
    ws.send(request)
    result = json.loads(ws.recv())
    return result


def get_bid_ask_volumes(orderbook):
    bids = np.asarray(orderbook['bids']).astype(float)
    asks = np.asarray(orderbook['asks']).astype(float)
    result = {}
    result['asks_within_1percent'] = 0
    result['asks_within_5percent'] = 0
    result['asks_within_10percent'] = 0
    result['bids_within_1percent'] = 0
    result['bids_within_5percent'] = 0
    result['bids_within_10percent'] = 0
    result['bids_volume_within_1percent'] = 0
    result['asks_volume_within_1percent'] = 0
    result['bids_volume_within_5percent'] = 0
    result['asks_volume_within_5percent'] = 0
    result['bids_volume_within_10percent'] = 0
    result['asks_volume_within_10percent'] = 0
    for row in bids:
        if row[0] > bids[0][0]*.99:
            result['bids_volume_within_1percent'] += row[1]
            result['bids_within_1percent'] += 1
        if row[0] > bids[0][0]*.95:
            result['bids_volume_within_5percent'] += row[1]
            result['bids_within_5percent'] += 1
        if row[0] > bids[0][0]*.9:
            result['bids_volume_within_10percent'] += row[1]
            result['bids_within_10percent'] += 1
            

    for row in asks:
        if row[0] < asks[0][0]*1.01:
            result['asks_volume_within_1percent'] += row[1]
            result['asks_within_1percent'] += 1
        if row[0] < asks[0][0]*1.05:
            result['asks_volume_within_5percent'] += row[1]
            result['asks_within_5percent'] += 1
        if row[0] < asks[0][0]*1.1:
            result['asks_volume_within_10percent'] += row[1]
            result['asks_within_10percent'] += 1
    return result


def insertRows(connection, cursor, data):
    timestamp = int(time.time())
    try:
        query = "REPLACE INTO gdax_orderbook (time, " \
                "bids_within_1percent, asks_within_1percent," \
                "bids_within_5percent, asks_within_5percent," \
                "bids_within_10percent, asks_within_10percent," \
                "bids_volume_within_1percent, asks_volume_within_1percent," \
                "bids_volume_within_5percent, asks_volume_within_5percent," \
                "bids_volume_within_10percent, asks_volume_within_10percent)" \
                "VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12})".format(
                    timestamp, 
                    data['bids_within_1percent'],
                    data['asks_within_1percent'],
                    data['bids_within_5percent'],
                    data['asks_within_5percent'],
                    data['bids_within_10percent'],
                    data['asks_within_10percent'],
                    data['bids_volume_within_1percent'],
                    data['asks_volume_within_1percent'],
                    data['bids_volume_within_5percent'],
                    data['asks_volume_within_5percent'],
                    data['bids_volume_within_10percent'],
                    data['asks_volume_within_10percent'])
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        logging.error("Error: Could not insert row. {0}".format(e))


def lambda_handler(event, context):
    orderbook = get_orderbook()
    volumes = get_bid_ask_volumes(orderbook)
    connection = openMySQLConnection()
    cursor = connection.cursor()
    insertRows(connection, cursor, volumes)
    cursor.close()
    connection.close()
    return 'Success'
    
