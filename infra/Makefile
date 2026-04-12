REGION ?= us-east-1
ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text)
IMAGE_NAME = okgl-pipeline
ECR_URL = $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(IMAGE_NAME)

deploy-dynamodb:
	aws cloudformation deploy \
		--template-file cloudformation/dynamodb.yml \
		--stack-name okgl-metadata \
		--region $(REGION)

deploy-iam:
	aws cloudformation deploy \
		--template-file cloudformation/iam.yml \
		--stack-name okgl-iam \
		--capabilities CAPABILITY_NAMED_IAM \
		--region $(REGION)

deploy-glue:
	aws cloudformation deploy \
		--template-file cloudformation/glue.yml \
		--stack-name okgl-glue \
		--region $(REGION)

deploy-ecr:
	aws cloudformation deploy \
		--template-file cloudformation/ecr.yml \
		--stack-name okgl-ecr \
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
