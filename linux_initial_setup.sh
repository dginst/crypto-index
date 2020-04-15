#Initial Setup
sudo apt-get update
sudo apt-get install python3
sudo apt install python3-pip

#Package Download

pip3 install --user pandas requests pymongo

#Install MongoDB

wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list
sudo apt-get update
sudo apt-get install -y mongodb-org

#Start MongoDB 
sudo systemctl start mongod
#$ sudo systemctl status mongod  # you can run this command to check tha mongodb status, then ctrl + c to exit


#Clone the repo 

git clone https://github.com/dginst/crypto-index.git