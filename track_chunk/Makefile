help:
	@echo "Usage:"
	@echo "  make deploy            Redeploy the function"
	@echo "  make teardown          Tear down the function"
	@echo "  make logs              Show the last 50 logs for the function"
	@echo "  make env               Print the environment variables"
	@echo "  make help              Show this help message"

function:
	@echo "Deploying function..."
	./deploy-function.sh
	@echo "Done deploying function."

deploy: function

teardown:
	@echo "Tearing down function..."
	./teardown.sh
	@echo "Done tearing down function."

logs:
	gcloud functions logs read update_chunk_status --region=us-central1 --limit=50 | less

env:
	@echo "Printing environment variables..."
	./print-env.sh
	@echo "Done printing environment variables."
