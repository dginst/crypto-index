#Initial Setup
sudo apt-get update
sudo apt install python3-pip

#Package Download

pip3 --user install pandas requests pymongo

#Install MongoDB

wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list
sudo apt-get update
sudo apt-get install -y mongodb-org

#Start MongoDB and check status
sudo systemctl start mongod
sudo systemctl status mongod

#Clone the repo 

git clone https://github.com/dginst/crypto-index.git