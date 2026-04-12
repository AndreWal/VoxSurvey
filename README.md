# DD Survey Pipeline

This repository provides a compact data engineering pipeline for working with VOX survey data from Swissvotes. It is designed to download and ingest raw survey files, harmonize a core set of variables in Python, and store the cleaned output in PostgreSQL for downstream statistical analysis and machine learning.

The project focuses on turning historically valuable but structurally inconsistent survey data into a reusable analytical asset. By standardizing key variables across waves, it creates a cleaner foundation for exploratory research, reproducible quantitative workflows, model development, and longitudinal comparisons.

## What This Repository Does

- downloads or ingests VOX survey data from Swissvotes
- transforms raw files into a harmonized schema
- validates the processed output
- loads curated data into a PostgreSQL database
- supports later use in statistical analysis and machine learning workflows

## Pipeline Overview

The workflow is organized as a small end-to-end pipeline:

1. `download`: collect raw survey data files
2. `transform`: clean and harmonize a shared set of core variables
3. `validate`: check the processed data before loading
4. `load`: write the final dataset into PostgreSQL

The repository includes Python source code in `src/`, workflow notebooks in `notebook/`, raw and processed data folders under `data/`, and Docker configuration for standing up PostgreSQL and pgAdmin locally.

## Quick Start

```bash
cp .env.example .env
make setup
make up
```

After the database is running, the pipeline components can be executed from the scripts in `notebook/` or the modules in `src/`, depending on whether you want an exploratory or more programmatic workflow.

## Project Structure

- `src/`: core Python modules for downloading, transforming, validating, and loading survey data
- `notebook/`: lightweight workflow scripts for stepping through each pipeline stage
- `data/raw/`: local storage for unprocessed source files
- `data/processed/`: local storage for harmonized outputs
- `docker-compose.yml`: local PostgreSQL and pgAdmin services
- `.env.example`: template for the environment variables required by Docker services

## Why It Matters

Swissvotes survey data is rich in political and social information, but it becomes substantially more useful once it is standardized and stored in a queryable database. This project bridges that gap and makes the data easier to analyze, compare, and model at scale.