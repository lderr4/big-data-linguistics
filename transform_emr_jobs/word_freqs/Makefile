


make test:
	echo 'HELLO WORLD!'

build-cluster:
	aws emr create-cluster \
		--name "reddit-comments-spark-444449" \
		--release-label "emr-6.15.0" \
		--service-role "arn:aws:iam::207567776666:role/EMR_DefaultRole" \
		--unhealthy-node-replacement \
		--ec2-attributes "{\"InstanceProfile\":\"EMR_EC2_DefaultRole\",\"EmrManagedMasterSecurityGroup\":\"sg-0f503d7c7ff9d61a8\",\"EmrManagedSlaveSecurityGroup\":\"sg-0cab1ab0665711cde\",\"KeyName\":\"final_key\",\"SubnetIds\":[\"subnet-0400832d8c6f464a2\"]}" \
		--applications Name=Hadoop Name=Hive Name=JupyterEnterpriseGateway Name=Livy Name=Spark \
		--instance-groups '[ \
			{ \
				"InstanceCount": 1, \
				"InstanceGroupType": "MASTER", \
				"Name": "Primary", \
				"InstanceType": "m5.xlarge", \
				"EbsConfiguration": { \
					"EbsBlockDeviceConfigs": [ \
						{ \
							"VolumeSpecification": { "VolumeType": "gp2", "SizeInGB": 32 }, \
							"VolumesPerInstance": 2 \
						} \
					] \
				} \
			}, \
			{ \
				"InstanceCount": 2, \
				"InstanceGroupType": "CORE", \
				"Name": "Core", \
				"InstanceType": "m5.xlarge", \
				"EbsConfiguration": { \
					"EbsBlockDeviceConfigs": [ \
						{ \
							"VolumeSpecification": { "VolumeType": "gp2", "SizeInGB": 32 }, \
							"VolumesPerInstance": 2 \
						} \
					] \
				} \
			}, \
			{ \
				"InstanceCount": 8, \
				"InstanceGroupType": "TASK", \
				"Name": "Task - Spot", \
				"InstanceType": "m5.xlarge", \
				"BidPrice": "0.2", \
				"EbsConfiguration": { \
					"EbsBlockDeviceConfigs": [ \
						{ \
							"VolumeSpecification": { "VolumeType": "gp2", "SizeInGB": 32 }, \
							"VolumesPerInstance": 2 \
						} \
					] \
				} \
			} \
		]' \
		--scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
		--auto-termination-policy '{"IdleTimeout":3600}' \
		--bootstrap-actions '[{"Path":"s3://reddit-comments-444449/bootstrap/python_package_installation.sh","Name":"CustomPythonInstall"}]' \
		--region "us-east-1"


get-cluster-id:
	@CLUSTER_ID=$(shell aws emr list-clusters --active --query "Clusters[0].Id" --output text); \
	echo $$CLUSTER_ID

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

scp-file-into-master:
	@echo "Getting DNS for cluster ID: $(id)"
	MASTER_DNS=$(shell aws emr describe-cluster --cluster-id $(id) --query "Cluster.MasterPublicDnsName" --output text); \
	echo "Connecting to cluster $(id) at $$MASTER_DNS"; \
	scp -i /Users/lucasderr/Desktop/ski-resort-weather/final_key.pem $(path) hadoop@$$MASTER_DNS:/home/hadoop/
