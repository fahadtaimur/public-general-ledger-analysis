REGION ?= us-east-1

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
		--region us-east-1
