.PHONY: build-container build-flex-template deploy-function destroy print-logs print-env help deploy-all

deploy-all: build-container build-flex-template deploy-function

help:
	@echo "Usage:"
	@echo "  make build-container      Build and push the container to GCR"
	@echo "  make build-flex-template  Build the Flex template"
	@echo "  make deploy-function      Deploy the function"
	@echo "  make deploy-all           Redeploy the container, function, and Flex template"
	@echo "  make destroy              Tear down the function"
	@echo "  make print-env            Print the environment variables"
	@echo "  make print-logs           Show the last 50 logs for the function"
	@echo "  make help                 Show this help message"

build-container:
	@echo "Building container..."
	./build-container.sh
	@echo "Done building container."

build-flex-template:
	@echo "Building flex template..."
	./build-flex-template.sh
	@echo "Done building Flex template."

deploy-function:
	@echo "Deploying function..."
	./deploy-function.sh
	@echo "Done deploying function."

destroy:
	@echo "Tearing down function..."
	./teardown.sh
	@echo "Done tearing down function."

print-logs:
	gcloud functions logs read trigger_stage_chunk --region=us-central1 --limit=50 | less

print-env:
	@echo "Printing environment variables..."
	./print-env.sh
	@echo "Done printing environment variables."
