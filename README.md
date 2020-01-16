<p align="center">
  <img src="https://dgi.io/img/logo/dgi-logo.svg?raw=true" alt="DGI Logo"/>
</p>


## DGI Crypto-Asset Index

The DGI crypto Index is a volume-weighted index and is composed of the most relevant crypto-assets in terms of liquidity and qualitative criteria. Designed and developed by a heterogeneous group of professionals with significant experience and relevant expertise related to Financial Benchmarks, Crypto–Assets and Financial Industry, the DGI Crypto Index is intended to give to private and institutional investors a replicable tool that best represent and synthesize the Crypto-assets markets. This Benchmark is denominated in USD and it is simulated from 2016/01/01.

**Key Features:**

* use volume as resource for weight computation
* selected exchanges based on security, real-volume and law compliance
* selected constituents on quantitative and qualitative criteria
* designed to be replicable by investors thanks to the buy and hold daily solution.

## Implementation

# Data Download

The software downloads the daily crypto-asset data in terms of trade volume and price of the 8 selected pricing sources that proved to be reliable in matter of real volumes and legal compliance: 
BitFlyer, BitStamp, Bittrex, Coinbase-Pro, Gemini, itBit, Kraken, Pooniex. The Data of these Exchanges are downloaded through the REST API of the website https://cryptowat.ch/ except for itBit's data that are downloaded through the REST API of itBit website.
Cryptowatch, demonstrated to be a reliable data bank for Crypto-Assets; It is owned by the Kraken exchange and was founded in 2014 by Artur Sapek.

Because the Index denomination is in USD all prices from the non-USD  are reported in USD; to do so, all the daily relevant exchange rates of the  Fiat/USD pairs and Fiat/StableCoin pairs are obtained respectively, from the European Central Bank (ECB) Website and the data provider CoinGecko; these pairs are  later used to convert all the prices in USD.

All the Crypto/Crypto pairs will be taken into account at a later moment.


Is it possible to find the download functions in the utils/data_download.py file.


# Data Setup

Once the relevant data are downloaded, they're processed.  

# Index Computation