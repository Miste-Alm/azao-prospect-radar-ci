# -----------------------------------------------------------------------------
# Variables
# -----------------------------------------------------------------------------
LAMBDA_ZIP  := dist/package.zip          # archive finale
SRC_FILES   := pipeline_lambda.py requirements.txt
PYTHON_BIN  ?= python3

# -----------------------------------------------------------------------------
# Commandes usuelles
# -----------------------------------------------------------------------------
.PHONY: help deps test clean package zip

help:               ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?##"} {printf "\033[36m%-12s\033[0m %s\n", $$1, $$2}'

deps:               ## Installe dépendances de prod
	$(PYTHON_BIN) -m pip install -r requirements.txt

test-deps:          ## Installe dépendances de test
	$(PYTHON_BIN) -m pip install -r tests/requirements_test.txt

test: deps test-deps ## Lance Pytest
	pytest -q

clean:              ## Nettoie dist/ & caches
	rm -rf dist __pycache__ */__pycache__ .pytest_cache

# -----------------------------------------------------------------------------
# Packaging Lambda
# -----------------------------------------------------------------------------
package: zip        ## Construit le zip Lambda dans dist/
	@echo "✅  Zip généré : $(LAMBDA_ZIP)"

zip: $(LAMBDA_ZIP)

dist:
	mkdir -p dist

$(LAMBDA_ZIP): $(SRC_FILES) | dist
	@echo "🔄  Zippage sources → $@"
	zip -j $@ $^IMAGE = azao-lambda-local
build:        docker build -t $(IMAGE) .
test: build   docker run --rm $(IMAGE) sam local invoke --event tests/event.json
package:      zip -r dist/pipeline_lambda.zip pipeline_lambda.py requirements.txt
