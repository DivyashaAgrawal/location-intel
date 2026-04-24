.DEFAULT_GOAL := help

PY      ?= python3
VENV    := venv
ACT     := . $(VENV)/bin/activate

.PHONY: help setup env install lint type run warm export clean

help:  ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Location Intelligence - commands\n\n"} \
	      /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

setup:  ## One-shot setup: venv, deps, Ollama + model, Redis install + start, .env, DB init
	@./setup.sh

env:  ## Open .env in $EDITOR (vi if unset). Seeds from .env.example if missing.
	@if [ ! -f .env ] && [ -f .env.example ]; then cp .env.example .env; echo "[make] Seeded .env from .env.example"; fi
	@$${EDITOR:-vi} .env 

install:  ## Install deps into existing venv (runtime + lint tools)
	$(ACT) && pip install -e '.[dev]'

lint:  ## Run code quality checks (ruff)
	$(ACT) && ruff check .

type:  ## Run type checks (mypy)
	$(ACT) && mypy src/

run:  ## Start the web app (streamlit)
	$(ACT) && streamlit run src/ui/streamlit_app.py

warm:  ## Pre-warm the cache. Usage: make warm BRANDS="Dominos Pizza" CITIES="Delhi,Mumbai"
	$(ACT) && python -m src.tools.warm_cache --brands "$(BRANDS)" --cities "$(CITIES)"

export:  ## Export DB. Usage: make export FORMAT=csv OUT=/tmp/stores.csv
	$(ACT) && python -m src.tools.export_data --format "$(FORMAT)" --output "$(OUT)"

clean:  ## Remove caches, coverage, __pycache__ (keeps venv + DB)
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .coverage .mypy_cache .ruff_cache htmlcov
