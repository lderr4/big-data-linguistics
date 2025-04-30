sudo apt update -y
sudo apt install unzip curl transmission -y

# download the .torrent file
curl -L \
https://academictorrents.com/download/7690f71ea949b868080401c749e878f98de34d3d.torrent \
-o reddit_comments.torrent 

# AWS CLI Setup
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" 
unzip awscliv2.zip
sudo ./aws/install

aws configure # follow the prompt and put your credentials in

# Download the dataset ~4 hours
transmission-cli reddit_comments.torrent 

# Create an S3 Bucket to upload the dataset
aws s3api create-bucket --bucket reddit-comments-444449 --region us-east-1

# Upload the dataset to the bucket ~20 minutes
aws s3 cp /mnt/ebs/reddit_data s3://reddit-comments-444449/raw --recursive