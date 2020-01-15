<p align="center">
  <img src="https://dgi.io/img/logo/dgi-logo.svg?raw=true" alt="DGI Logo"/>
</p>


## DGI Crypto-Asset Index

The DGI crypto Index is a volume-weighted index and is composed of the most relevant crypto-assets in terms of liquidity and qualitative criteria. Designed and developed by a heterogeneous group of professionals with significant experience and relevant expertise related to Financial Benchmarks, Crypto–Assets and Financial Industry, the DGI Crypto Index is intended to give to private and institutional investors a replicable tool that best represent and synthesize the Crypto-assets markets.

**Key Features:**

* use volume as a resource for weight computation
* selected exchanges based on security, real-volume and law compliance
* selected constituents on quantitative and qualitative criteria
* designed to be replicable by investors thanks to the buy and hold daily solution.

## Implementation

# Data Download

The software downloads the daily crypto-asset data in terms of trade volume and price of the 8 selected pricing sources that proved to be reliable in matter of real volumes and legal compliance: 

 <div class="image-block-outer-wrapper layout-caption-below design-layout-inline combination-animation-none individual-animation-none individual-text-animation-none">
        <div class="intrinsic" style="max-width:800.0px;">
            <div style="padding-bottom:15.625%;" class="image-block-wrapper   has-aspect-ratio" data-animation-tier="1" data-description="" >
              <noscript><img src="https://images.squarespace-cdn.com/content/v1/58daeda9414fb59dd3de3ccd/1555698546039-MYT45RC6TLXVMKYBWCFT/ke17ZwdGBToddI8pDm48kLK43TW2B3oKMB1ocDFW8HtZw-zPPgdn4jUwVcJE1ZvWQUxwkmyExglNqGp0IvTJZamWLI2zvYWH8K3-s_4yszcp2ryTI0HqTOaaUohrI8PIroehUutD7l49RnYZal-1OuVWBm1siO-GLcUj7cncWEI/Poloniex-logo-800px.png" alt="Poloniex-logo-800px.png" /></noscript><img class="thumb-image" data-src="https://images.squarespace-cdn.com/content/v1/58daeda9414fb59dd3de3ccd/1555698546039-MYT45RC6TLXVMKYBWCFT/ke17ZwdGBToddI8pDm48kLK43TW2B3oKMB1ocDFW8HtZw-zPPgdn4jUwVcJE1ZvWQUxwkmyExglNqGp0IvTJZamWLI2zvYWH8K3-s_4yszcp2ryTI0HqTOaaUohrI8PIroehUutD7l49RnYZal-1OuVWBm1siO-GLcUj7cncWEI/Poloniex-logo-800px.png" data-image="https://images.squarespace-cdn.com/content/v1/58daeda9414fb59dd3de3ccd/1555698546039-MYT45RC6TLXVMKYBWCFT/ke17ZwdGBToddI8pDm48kLK43TW2B3oKMB1ocDFW8HtZw-zPPgdn4jUwVcJE1ZvWQUxwkmyExglNqGp0IvTJZamWLI2zvYWH8K3-s_4yszcp2ryTI0HqTOaaUohrI8PIroehUutD7l49RnYZal-1OuVWBm1siO-GLcUj7cncWEI/Poloniex-logo-800px.png" data-image-dimensions="800x125" data-image-focal-point="0.5,0.5" alt="Poloniex-logo-800px.png" data-load="false" data-image-id="5cba1371e79c70f84317b031" data-type="image" />
            </div>
          </div>
      </div>



BitFlyer, BitStamp, Bittrex, Coinbase-Pro, Gemini, itBit, Kraken, Pooniex. The Data of these Exchanges are downloaded through the REST API of the website https://cryptowat.ch/ except for itBit's data that are downloaded through the REST API of itBit website.

Is it possible to find the functions in the utils/data_download.py file.

# Data Setup


# Index Computation