<p align="center">
  <img src="https://dgi.io/img/logo/dgi-logo.svg?raw=true" alt="DGI Logo"/>
</p>


# DGI Crypto-Asset Index

The DGI crypto Index is a volume-weighted index and is composed of the most relevant crypto-assets in terms of liquidity and qualitative criteria. Designed and developed by an heterogeneous group of professionals with significant experience and relevant expertise related to Financial Benchmarks, Crypto–Assets and Financial Industry, the DGI Crypto Index is intended to provide to private and institutional investors a replicable tool that best represent and synthesize the Crypto-assets market. This Benchmark is denominated in USD and it is simulated from 2016/01/01.

**Key Features:**

* use volume as resource for weight computation
* selected pricing sources based on security, real-volume and law compliance
* selected constituents on quantitative and qualitative criteria
* designed to be replicable by investors thanks to the buy and hold daily solution.

# Implementation

## Index Definition Process - Workflow

<p align="center">
  <img src="https://github.com/dginst/crypto-index/blob/master/WF.png?raw=true" alt="Crypto-Index WF"/>
</p>


## Data Download

The software downloads the daily crypto-asset data in terms of trade volume and price of the 8 selected pricing sources that proved to be reliable in matter of real volumes and legal compliance: 

- BitFlyer
- BitStamp
- Bittrex
- Coinbase-Pro
- Gemini
- itBit
- Poloniex

The Data of these Exchanges are downloaded through two main provider:

- the REST API of the Cryptowatch website https://cryptowat.ch/  
- the REST API of each single Exchange obtaining the daily ticker
  
Cryptowatch demonstrate to be a reliable data bank for Crypto-Assets, it is owned by the Kraken exchange and was founded in 2014 by Artur Sapek. Note that Cryptowatch has the historical values of all the selected Exchanges except for itBit's data that are downloaded only through the REST API of itBit website.

Because the Index denomination is in USD all prices from the non-USD pairs are reported in USD; to do so, all the daily relevant exchange-rates of the Fiat/USD pairs and Fiat/StableCoin pairs are obtained respectively, from the European Central Bank (ECB) Website and the data provider CoinGecko; 

The Data Download procesess consists in downloading the data on daily basis from the described API and store it, as raw data, into the database Mongo DB.

The Mongo DB chosen tree structure is the following:

  - database name: Crypto - Index
  - collection names: CW_raw, XCH_raw, ECB_raw
  - documents: equivalent of a row, can be identified through specific "fields" (ex {'Pair': 'BTCUSD', 'Exchange': 'Kraken'})


Is it possible to find the download functions in the utils/data_download.py file.


## Data Management

The Data Management process mainly consists in: retriving the raw data from Mongo DB through querying, manipulating the data and structuring them as needed for the Index computation.

### Querying from Mongo DB

### Historical Series Manipulation

### Querying from Mongo DB
Once the relevant data are downloaded, they're processed.  

# Index Computation

Is possible to split the index calculation in six main parts.

1) First logic matrix computation

  The first logic matrix has the role of a first screening of all the crypto-assets in order to decide wheter or not the single crypto-asset can be eligible, for the index composition, in a determined interval of time. The interval of interest goes from each start date of the quarter to the end date of the quarter itself; follows that the specified interval is the period of application where a cryptoasset can be considered eligible. For the actual computation of the eligible cryptoassets is considered the period between the start date of each quarter and the day before the board day meeting of the quarter itself. Here the rule: if, for a single cryptoasset, one single exchange has traded more than the 80% of the total volume of the period, the cryptoasset is excluded.

2) Exponential Weighted Movin Average (EWMA) computation

  The ewma is performed on the traded volume levels and with a moving average period of 90 days. The code takes the data frame containing the total daily volume for each crypto-asset (cryptos as columns, days as rows) and compute the daily ewma considering the last 90 days volume values multiplied per the relative lambda, and then summarizing them.
  The index uses a fixed lambda value of 0.94  that decraeses exponentially starting from a value of 0.94 and followuing the formula:

    daily_lambda = (1 - 0.94) * (0.94 ^ (index))

  where the index is the value of intergers in the range (89, 0).
  The ewma values has two different usages: the computation of the second logic matrix and the computation of the quarterly weights.

3) Second logic matrix computation

  The second logic matrix is the second and last eligibility check that is needed in order to achieve a correct and consistent composition of the index.
  The idea is to compare, for each crytpoasset, the aggregate ewma volume of the specified period and then find the percentage that the single cryptoasset has respect to the totoal sum.
  The first step is to leave out, for each quarter, the cryptoassets that has been eliminated due to the first logic matrix; then the code takes the ewma values for the period between the start date of the quarter and the day before the board meeting day, sum them for sinle cryptoassets and compute the relative percentage of each one.
  If the relative percentage of a cryptoasset in less than 2%, the crytpoasset will be considered non eligible for the quarter.
  back the % of the EWMA-volume of any single cryptoasset compared to the aggregate EWMA-volume

4) Quarter Weights computation

  The weights computation is performed for each quarter on the day before the board meeting day. 
  The first step is to leave out from the ewma dataframe all the cryptoassets that have been ideintified as non eligible with the logic matrices; done that the code simply find the relative weights of the cryptoassets ewma: these are the weights of the next quarter.

5) Synthetic Market Cap computation



6) Initial Divisor computation

