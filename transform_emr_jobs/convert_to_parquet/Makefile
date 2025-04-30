test:
	echo "hello world"

build-cluster:
	aws emr create-cluster \
    --name "reddit-comments-spark-444449" \
    --release-label "emr-6.15.0" \
    --service-role "arn:aws:iam::207567776666:role/EMR_DefaultRole" \
    --unhealthy-node-replacement \
    --ec2-attributes '{\
        "InstanceProfile": "EMR_EC2_DefaultRole",\
        "EmrManagedMasterSecurityGroup": "sg-0f503d7c7ff9d61a8",\
        "EmrManagedSlaveSecurityGroup": "sg-0cab1ab0665711cde",\
        "KeyName": "final_key",\
        "SubnetIds": ["subnet-0400832d8c6f464a2"]\
    }' \
    --applications Name=Hadoop Name=Spark \
    --instance-groups '[\
            {\
                "InstanceCount": 1,\
                "InstanceGroupType": "MASTER",\
                "Name": "Primary",\
                "InstanceType": "m5.xlarge",\
                "EbsConfiguration": {\
                    "EbsBlockDeviceConfigs": [\
                        {\
                            "VolumeSpecification": { "VolumeType": "gp2", "SizeInGB": 32 },\
                            "VolumesPerInstance": 2\
                        }\
                    ]\
                }\
            },\
            {\
                "InstanceCount": 4,\
                "InstanceGroupType": "CORE",\
                "Name": "Core - Spot",\
                "InstanceType": "m5.xlarge",\
                "BidPrice": "0.07",\
                "EbsConfiguration": {\
                    "EbsBlockDeviceConfigs": [\
                        {\
                            "VolumeSpecification": { "VolumeType": "gp2", "SizeInGB": 32 },\
                            "VolumesPerInstance": 2\
                        }\
                    ]\
                }\
            },\
            {\
                "InstanceCount": 10,\
                "InstanceGroupType": "TASK",\
                "Name": "Task - Spot",\
                "InstanceType": "m5.xlarge",\
                "BidPrice": "0.07",\
                "EbsConfiguration": {\
                    "EbsBlockDeviceConfigs": [\
                        {\
                            "VolumeSpecification": { "VolumeType": "gp2", "SizeInGB": 32 },\
                            "VolumesPerInstance": 2\
                        }\
                    ]\
                }\
            }\
        ]' \
    --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
    --auto-termination-policy '{"IdleTimeout":3600}' \
    --bootstrap-actions '[{\
        "Path": "s3://reddit-comments-444449/bootstrap/bz2_to_parquet/bootstrap.sh",\
        "Name": "CustomPythonInstall"\
    }]' \
    --region "us-east-1"

upload-bootstrap-script:
	aws s3 cp bootstrap.sh s3://reddit-comments-444449/bootstrap/sentiment/bootstrap.sh


check-spot-price:
	aws ec2 describe-spot-price-history \
		--instance-types m5.xlarge \
		--product-description "Linux/UNIX" \
		--start-time "$$(date -u -v -10H +%Y-%m-%dT%H:%M:%SZ)" \
		--availability-zone us-east-1a \
		--max-items 10 \
		--query 'SpotPriceHistory[*].[Timestamp,SpotPrice]' \
		--output table
		
get-cluster-status:
	aws emr describe-cluster --cluster-id $(id) \
	--query "Cluster.Status.State" \
	--output text

get-master-dns:
	aws emr describe-cluster --cluster-id $(id) --query "Cluster.MasterPublicDnsName" --output text

# This target SSHs into the master node using the cluster ID
ssh-into-master:
	@echo "Getting DNS for cluster ID: $(id)"
	MASTER_DNS=$(shell aws emr describe-cluster --cluster-id $(id) --query "Cluster.MasterPublicDnsName" --output text); \
	echo "Connecting to cluster $(id) at $$MASTER_DNS"; \
	ssh -i /Users/lucasderr/Desktop/ski-resort-weather/final_key.pem hadoop@$$MASTER_DNS

