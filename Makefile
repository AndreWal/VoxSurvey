SHELL := /bin/bash

.PHONY: setup up down lint test pipeline

setup:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -U pip
	. .venv/bin/activate && pip install -e ".[dev]"

up:
	docker compose up -d

down:
	docker compose down

lint:
	. .venv/bin/activate && ruff check src/ tests/

test:
	. .venv/bin/activate && python -m pytest tests/ -v

pipeline:
	. .venv/bin/activate && cd notebook && \
	  python 01_download.py && \
	  python 02_transform.py && \
	  python 03_validate.py && \
	  python 04_load.py