# MLOps Batch Job (Trading Signal Pipeline)

## Overview
This project implements a minimal MLOps-style batch pipeline that:
- Loads configuration from YAML
- Processes OHLCV financial data
- Computes rolling mean on closing prices
- Generates binary trading signals
- Outputs structured metrics and logs
- Runs locally and inside Docker

---

## Features

### Reproducibility
- Config-driven execution (`config.yaml`)
- Fixed random seed

### Observability
- Structured logs (`run.log`)
- Machine-readable metrics (`metrics.json`)

### Deployment
- Fully Dockerized
- One-command execution

### Requirements
- pandas
- numpy
- pyyaml

---

## Usage

### ▶ Run Locally
```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
