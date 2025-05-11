sudo apt update && sudo apt upgrade -y
sudo apt install git -y
sudo apt install docker.io -y
sudo systemctl start docker
sudo systemctl enable docker

sudo usermod -aG docker $USER
newgrp docker

sudo apt install docker-compose -y

git clone https://github.com/lderr4/big-data-linguistics
cd big-data-linguistics

touch .env
sudo vim .env

# add correct values to .env file:
# RDS_PASSWORD=
# RDS_HOSTNAME=
# RDS_DB_NAME=
# RDS_USERNAME=
# RDS_PORT=
# API_URL=