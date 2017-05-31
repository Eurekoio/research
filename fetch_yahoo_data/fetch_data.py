"""Fetch Yahoo Finance Data Example

A quick script to fetch, parse and write financial data from Yahoo

"""

import urllib

TICKER = 'AAPL'
BASE_URL = "http://ichart.finance.yahoo.com/table.csv?s=%s"

# Fetch data from yahoo
response = urllib.urlopen(BASE_URL % TICKER)
data = response.readlines()  

# Parse and write data
for d in data:
    Date, Open, High, Low, Close, Volume, Adj_Close = d.split(',')
    data_string = '%s,%s,%s,%s,%s,%s,%s,%s' % (TICKER, Date, Open, High, Low, Close, Volume, Adj_Close)
    f = open('/tmp/data.txt', 'a')
    f.write(data_string)
    f.close()
