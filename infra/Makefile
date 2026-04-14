REGION ?= us-east-1
ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text)
IMAGE_NAME = okgl-pipeline
ECR_URL = $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(IMAGE_NAME)

deploy-iam:
	aws cloudformation deploy \
		--template-file cloudformation/iam.yml \
		--stack-name okgl-iam \
		--capabilities CAPABILITY_NAMED_IAM \
		--region $(REGION)

deploy-dynamodb:
	aws cloudformation deploy \
		--template-file cloudformation/dynamodb.yml \
		--stack-name okgl-metadata \
		--region $(REGION)

deploy-ecr:
	aws cloudformation deploy \
		--template-file cloudformation/ecr.yml \
		--stack-name okgl-ecr \
		--region $(REGION)

deploy-ecs:
	aws cloudformation deploy \
		--template-file cloudformation/ecs.yml \
		--stack-name okgl-ecs \
		--region $(REGION)

deploy-glue:
	aws cloudformation deploy \
		--template-file cloudformation/glue.yml \
		--stack-name okgl-glue \
		--region $(REGION)

deploy-athena:
	aws cloudformation deploy \
		--template-file cloudformation/athena.yml \
		--stack-name okgl-athena \
		--region $(REGION)

# container specific commands
build-image:
	docker build --no-cache --platform linux/amd64 -t $(IMAGE_NAME) ../

interactive-container:
	docker run --rm -it $(IMAGE_NAME) /bin/bash

ecr-login:
	aws ecr get-login-password --region $(REGION) | \
		docker login --username AWS --password-stdin $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com

push-image: ecr-login
	docker tag $(IMAGE_NAME):latest $(ECR_URL):latest
	docker push $(ECR_URL):latest

build-push-image: build-image push-image

# Pipeline
# Manual usage: make run-task SUBNET_ID=subnet-xxx SG_ID=sg-xxx
run-task:
	aws ecs run-task \
		--cluster okgl-cluster \
		--task-definition okgl-pipeline \
		--launch-type FARGATE \
		--network-configuration "awsvpcConfiguration={subnets=[$(SUBNET_ID)],securityGroups=[$(SG_ID)],assignPublicIp=ENABLED}" \
		--region $(REGION)

logs:
	aws logs tail /ecs/okgl-pipeline --follow --region $(REGION)

get-vpc-ids:
	@echo "SubnetId:"
	@aws ec2 describe-subnets --filters "Name=default-for-az,Values=true" --query "Subnets[0].SubnetId" --output text --region $(REGION)
	@echo "SecurityGroupId:"
	@aws ec2 describe-security-groups --filters "Name=group-name,Values=default" --query "SecurityGroups[0].GroupId" --output text --region $(REGION)
