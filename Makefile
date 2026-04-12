SHELL := /bin/bash

.PHONY: setup up down

setup:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -U pip
	. .venv/bin/activate && pip install -e ".[dev]"

up:
	docker compose up -d

down:
	docker compose down