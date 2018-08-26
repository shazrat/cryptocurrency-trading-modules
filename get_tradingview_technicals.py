#!/usr/bin/python
"""
Name: get_tradingview_signals

This script uses Selenium to load Tradingview's technical analysis page for 
the cryptocurrencies listed in 'MARKETS'.
(ex. https://www.tradingview.com/symbols/BCHUSD/technicals/)
It then selects the total number of buy/sell/neutral signals listed on the page 
for each of 8 time intervals, and saves the information to an AWS DynamoDB table
for further usage and analysis.

Python package requirements: 
  selenium
  boto3

Infrastructure requirements: 
  AWS CLI pre-configured for Boto3 usage
  AWS DynamoDB table already created.
  
Local files:
  chromedriver for your host OS
"""

from __future__ import print_function
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import boto3

__author__ = 'Shiraz Hazrat'
__email__ = 'shiraz@shirazhazrat.com'
__copyright__ = 'Copyright 2018, Fractal Strategies Inc.'

BASE_URL = 'https://www.tradingview.com/symbols/{}/technicals/'
MARKETS = ['BCHUSD', 'BTCUSD', 'ETHUSD', 'LTCUSD']
DYNAMO_TABLE_NAME = 'tradingview-signals'
TIME_INTERVALS = ['1_min', '5_min', '15_min', '1_hour', '4_hour', '1_day',
          '1_week', '1_month']


def get_signals(MARKET):
  """ Loads TradingView technicals website. Grabs signals, and returns a
    dict with the data.
    
    Args:
      MARKET (string): Ex. 'BCHUSD'
      
    Returns:
      signals (dict): {'1_min': {'sell_count': 12, 'buy_count': 3, ... },
                       '5_min': {'sell_count': 9, 'buy_count': 2, ... }, ... }
  """
  signals = {}
  signals['market'] = MARKET
  signals['time'] = int(time.time())

  # Define settings for Selenium to open a Chrome window.
  chrome_options = Options()
  chrome_options.add_argument("--headless")
  chrome_options.add_argument("--window-size=1920x1080")
  driver = webdriver.Chrome('./chromedriver', chrome_options=chrome_options)
  url = BASE_URL.format(MARKET)
  driver.get(url)
  time.sleep(2)

  button_xpaths = []
  for i in range(1,9):
    xpath = '//*[@id="technicals-root"]/div/div/div[1]/div/div[{}]/div'.format(i)
    button = driver.find_element_by_xpath(xpath)
    button.click()

    time.sleep(1)
    signal_xpath = '//*[@id="technicals-root"]/div/div/div[2]/div[2]/span[2]'
    BASE_XPATH = '//*[@id="technicals-root"]/div/div/div[2]/div[2]/div[2]/div[{}]/span[1]'
    sell_count_xpath = '//*[@id="technicals-root"]/div/div/div[2]/div[2]/div[2]/div[1]/span[1]'
    neutral_count_xpath = '//*[@id="technicals-root"]/div/div/div[2]/div[2]/div[2]/div[2]/span[1]'
    buy_count_xpath = '//*[@id="technicals-root"]/div/div/div[2]/div[2]/div[2]/div[3]/span[1]'

    signal_word = driver.find_element_by_xpath(signal_xpath).text
    sell_count = driver.find_element_by_xpath(sell_count_xpath).text
    neutral_count = driver.find_element_by_xpath(neutral_count_xpath).text
    buy_count = driver.find_element_by_xpath(buy_count_xpath).text
    signals[TIME_INTERVALS[i-1]] = {
        "sell_count": int(sell_count),
        "neutral_count": int(neutral_count),
        "buy_count": int(buy_count),
        "signal_word": signal_word
        }

  return signals

def save_to_dynamo(signals):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(DYNAMO_TABLE_NAME)
  table.put_item(Item = signals)

def main():
  for market in MARKETS:
    signals = get_signals(market)
    save_to_dynamo(signals)

if __name__ == "__main__":
  # Execute only if run as a script
  main()
