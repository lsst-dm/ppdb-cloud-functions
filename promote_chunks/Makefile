help:
	@echo "Usage:"
	@echo "  make function          Deploy the function"
	@echo "  make deploy            Redeploy the container, function, and Flex template"
	@echo "  make teardown          Tear down the function"
	@echo "  make test              Test the function with a sample call"
	@echo "  make help              Show this help message"

function:
	@echo "Deploying function..."
	./deploy-function.sh
	@echo "Done deploying function."

schedule:
	@echo "Scheduling function..."
	./schedule.sh
	@echo "Done scheduling function."

deploy: function schedule

teardown:
	@echo "Tearing down function..."
	./teardown.sh
	@echo "Done tearing down function."

test:
	@echo "Testing function with a sample call..."
	./test-call.sh
	@echo
	@echo "Done testing function."
