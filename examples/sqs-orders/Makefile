# Uses your current AWS credentials. Set AWS_PROFILE to pick an account, e.g.
#   AWS_PROFILE=my-sandbox make deploy
TF := terraform -chdir=terraform
TAG := lambda-kit-go-e2e

.PHONY: build deploy test destroy leftovers

build:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -ldflags='-s -w' -o build/bootstrap .

deploy: build
	aws sts get-caller-identity --query Account --output text
	$(TF) init -input=false
	$(TF) plan -input=false -out=../build/tfplan
	$(TF) apply -input=false ../build/tfplan
	$(TF) output -json > build/outputs.json

test:
	go run ./e2e/loadgen
	go run ./e2e/verify

destroy: build
	$(TF) destroy -input=false -auto-approve
	$(MAKE) leftovers

# Lists anything still tagged Project=$(TAG). Empty output means a clean
# teardown. Deleted resources can linger in this listing for a few minutes.
leftovers:
	aws resourcegroupstaggingapi get-resources --region $$(jq -r .region.value build/outputs.json 2>/dev/null || echo us-east-1) \
		--tag-filters Key=Project,Values=$(TAG) --query 'ResourceTagMappingList[].ResourceARN' --output text
