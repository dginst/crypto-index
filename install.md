<p align="center">
  <img src="https://dgi.io/img/logo/dgi-logo.svg?raw=true" alt="DGI Logo"/>
</p>


# Install Guideline

Following these steps will allow you tu run the code on your machine and obtain the DGI Crypto-Index Time series.
Note that a needed requirement is the installation of MongoDB on your laptop.

### 1) Run ECB_hist_download.py

The file allows to download from the European Central Bank Websites the exchange rates for the following currencies:
* 'USD','GBP', 'CAD' and 'JPY'

Once downloaded, the code stores the retrieved data on MongoDB in the database "index" and collection "ecb_raw". 
It is possible to change the period of downlaod modifying the "start_period"

### 2) Run ECB_hist_setup.py

The file aims to complete the historical series of European Central Bank Websites exchange rates.
It retrieves the rates from MongoDB in the database "index" and collection "ecb_raw" then adds values for all the holidays and weekends, which are missing in the ECB websites series, simply repeating for each missing day the rates value of the last fixing day. 
Since the Crypto-Index express all of its values in USD, the code puts all the exchange rates in USD based values, converting the downloaded EUR based values. Thus the historical series will display the following exchange rates:
* EUR/USD, CAD/USD, GBP/USD, JPY/USD

The completed USD based historical series is saved back on MongoDb in the collection "ecb_clean".
It is possible to change the period of downlaod modifying the "start_period"

### 3) Run CW_hist_download.py

The file downloads from the CryotoWatch websites the market data of the following set of Cryptocurrencies: 

* 'ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV' and 'ETC';

and for each of the following fiat currencies, that form the Crypto-fiat pair (ex. btcusd, ethcad,...):

* 'gbp', 'usd', 'eur', 'cad' and 'jpy'.

all of the Crypto_fiat pairs is searched and downloaded from the following Exchanges:

* 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken' and 'bitflyer';

Once the downloads is done, the code saves the raw data on MongoDB in the database "index" and collection "rawdata". 
It is possible to change the period of downlaod modifying the "start_date"

### 4) Run CW_hist_setup.py

The file completes the historical series of Cryptocurrencies market data stored on MongoDB.
The main rules for the manipulation of raw data are the followings:

* if a certain Crypto-Fiat pair does not start at the beginning of the period but later, for example because the pair does not exist before, the code will put a series of zeros from the start date until the actual beginning of the series;

* if, for one or more date, a certain Crypto-pair series has missing data, the file will compute a value to insert using all the values displayed, for the same Crypto-Fiat pair, in the other exchanges. In particular the code cimputes the volume weighted average of the values of interest for each of the missing date;

* if, trying to fix a series as described above in the second point, the code finds out that just one exchange has the values for a certain Crypto-Fiat pair, the file will put a 0-values array for all the missing date;

Once the data is manipulated and the series has been homogeineized, the file will save the the historical series on MongoDB in the collection "cleandata".

### 5) Run CW_hist_conversion.py

The file retrieves data from MongoDB collection "cleandata" and, for each Crypto-Fiat historical series, converts the data into USD values using the ECB manipulated rates stored on MongoDB in the collection "ecb_clean".

Once everything is converted into USD the historical series is saved on MongoDB in the collection "converted_data"

### 6) Run TEST_MAIN.py

