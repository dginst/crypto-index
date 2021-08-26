<p align="center">
  <img src="https://dgi.io/img/logo/dgi-logo.svg?raw=true" alt="DGI Logo"/>
</p>


# Install Guideline

Following these steps will allow you tu run the code on your machine and obtain the DGI Crypto-Index Time series.


The repository CRYPTO-INDEX has the following structure:

CRYPTO-INDEX
|
|--- bin
        |--- EXC --> cointains the scripts to download daily from every single Exchange
        |
        |--- launcher_script --> cointains the scripts that allows to launch the index cmputatiob and the dashboard
|
|--- cryptoindex --> contains a set of file with functions
|
|--- __init__.py 
|
|--- install.md
|
|--- README.md
|
|--- requirements.txt
|
|--- setup.cfg
|
|--- setup.py

Note that a prerequisite is the installation of MongoDB on your laptop or server.

### 0) Virtual Environment setup 

Install the Crypto Index and its prerequisites into a
python virtual environment; e.g. from the root folder:

Bash shell

    python -m venv venv
    source venv/bin/activate
    pip install --upgrade -r requirements.txt
    pip install --upgrade -r requirements-dev.txt
    pip install --upgrade -e ./

Windows CMD or PowerShell:

    python -m venv venv
    .\venv\Scripts\activate
    pip install --upgrade -r requirements.txt
    pip install --upgrade -r requirements-dev.txt
    pip install --upgrade -e ./

Windows Git bash shell:

    python -m venv venv
    cd ./venv/Scripts
    . activate
    cd ../..
    pip install --upgrade -r requirements.txt
    pip install --upgrade -r requirements-dev.txt
    pip install --upgrade -e ./

### 1) Complete Operations

## 1.1) Upload Exchanges raw data

Create on Mongo DB a database called "index"
Upload all the crytpocurrencies raw data from the single Exchanges creating on Mongo DB a collection names "EXC_rawdata"

## 1.2) Run bin\launcher_script.index_total_launcher.py

## 1.3) Run bin\launcher_script\web_app_index.py

## 1.4) Run bin\launcher_script\index_daily_launcher.py on daily basis


### 2) Partial Operations

This way allows to avoid the downloads of the entire collection of exchange rates from ECB website and the history of cryptocurrencies from CryptoWatch

## 2.1) Upload Exchanges raw data

Create on Mongo DB a database called "index".
Upload all the crytpocurrencies raw data from the single Exchanges creating on Mongo DB a collection names "EXC_rawdata".
Upload all the exchange rates raw data creating on Mongo DB a collection named "ecb_raw".
Upload all the crypto raw data from CryptoWatch creating on Mongo DB a collection named "CW_rawdata".

## 2.2) Run bin\launcher_script.index_comp_launcher.py

## 2.3) Run bin\launcher_script\web_app_index.py

## 2.4) Run bin\launcher_script\index_daily_launcher.py on daily basis
