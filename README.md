# Cryptocurrency trading modules

Here you can find various scripts and functions that assist in creating a fully autonomous, algorithmic cryptocurrency trading system. I won't share all the goods, but hopefully this will help some of you out there ;)

## Get GDAX / Coinbase Pro Market Data
[get_gdax_candlesticks.py](get_gdax_candlesticks.py)

A lambda function which queries GDAX / Coinbase Pro for live and historical price data for all markets listed on their exchange, and saves it all to an AWS RDS-managed MySQL database. Collects down to the minute, and on every execution, gets additional 3 hours of historical data. It does this by determining the timestamp of the oldest record in the DB, and setting the start-end times accordingly for each data request.

Best paired with an AWS CloudWatch timer set to every 5 minutes, for easy and worry-free automatic data collection =D

## Get Tradingview Signals

[get_tradingview_technicals.py](get_tradingview_technicals.py)

Tradingview is a nifty website that offers a lot of information. Namely, they do a lot of technical analysis on cryptocurrencies. This can be done yourself, but in case you want to skip the hassle, you can scrape their results.

Example link: [https://tradingview.com/symbols/BCHUSD/technicals](https://tradingview.com/symbols/BCHUSD/technicals/)

The results are rendered through JavaScript on page load, and can't be fetched using simple Requests calls which just pull page source. A DOM renderer is needed, so Python Selenium does the trick.

Unfortunately, this can't be run in an AWS Lambda function as far as I've tried since it needs a full browser application along with it (i.e. chromedriver). I've set it up on a very light-weight linux VM that runs 24/7. Script is run every 5 minutes via cron.

Saves resulting data to AWS DynamoDB for quick polling by other scripts.