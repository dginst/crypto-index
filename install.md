To run the code on your machine you have to follow in order the next steps:

# * Run ECB_hist_download.py

The file download from the European Central Bank Websites the exchange rates for the currencies 'USD','GBP', 'CAD' and 'JPY'. Then store the retrieved data on MongoDB in the database "index" and collection "ecb_raw". is possible to change the period of downlaod modifying the "Start_Period"

# * Run ECB_hist_setup.py

The file aims to complete the historical series of European Central Bank Websites exchange rates.
It retrieves the rates from MongoDB in the database "index" and collection "ecb_raw" then add values for all the holidays and weekends simply copiyng the value of the last day with value. 
Morover the file takes the rates as EUR based exchange rates and returns USD based exchange rates.
The completed USD based historical series is saved back in MongoDb in the collection "ecb_clean" is possible to change the period of downlaod modifying the "Start_Period"

# * Run CW_hist_download.py

The file download from the CryotoWatch websites the market data of this set of Cryptocurrencies: 

* 'ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV' and 'ETC';

* from this set of exchanges: 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken' and 'bitflyer';

and for each fiat currenncies in this set:

* 'gbp', 'usd', 'eur', 'cad' and 'jpy'.

Once downloaded the file saves the raw data on MongoDB in the database "index" and collection "rawdata". It is possible to change the period of downlaod modifying the "start_date"

# * Run CW_hist_setup.py

The file completes the historical series of Cryptocurrencies market data stored on MongoDB
The main rules for the manipulation of raw data are the followings:

* if a certain Crypto-Fiat pair does not start at the beginning of the period but later, the file will put a series of zeros from the start period until the actual beginning of the series;

* if a certain data is missing in a certain date the file will compute a value to insert using all the values displayed, for the same Crypto-Fiat pair, in the other exchanges.

* if, trying to fix a series as described above, the code find out that just one exchange has the values for the wanted Crypto-Fiat pair, the file will put a 0-values array for all the missing date;

Once the data is manipulated and the series has been homogeineized, the file will save the the historical series on MongoDB in the collection "cleandata".

# * Run CW_hist_conversion.py

The file retrieves data from MongoDB collection "cleandata" and, for each Crypto-Fiat historical series, converts the data into USD values using the ECB manipulated rates stored on MongoDB in  the collection "ecb_clean".

Once everything is converted into USD the historical series is saved into MongoDB in the collection "converted_data"

# * Run TEST_MAIN.py