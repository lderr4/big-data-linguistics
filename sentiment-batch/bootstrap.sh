#!/bin/bash
sudo pip-3.7 install boto3==1.28.0 nltk==3.8.1 pandas pyarrow s3fs
sudo mkdir -p /tmp/nltk_data
sudo chmod -R 777 /tmp/nltk_data
sudo python3 -m nltk.downloader -d /tmp/nltk_data vader_lexicon