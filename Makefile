IMAGE = azao-lambda-local

build:
	docker build -t $(IMAGE) .

test: build
	docker run --rm $(IMAGE) sam local invoke --event tests/event.json

package:
	mkdir -p dist
	zip -j dist/pipeline_lambda.zip pipeline_lambda.py requirements.txt
