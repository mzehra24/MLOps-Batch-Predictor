import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import os

def setup_logger(log_file):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove old handlers (important fix)
    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config file not found")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        

    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing config key: {key}")

    return config

def load_data(input_path):
    if not os.path.exists(input_path):
        raise FileNotFoundError("Input file not found")

    df = pd.read_csv(input_path, sep=None, engine='python')

    # 🔥 CLEAN COLUMN NAMES (IMPORTANT FIX)
    df.columns = df.columns.str.strip().str.lower()

    print("Columns after cleaning:", df.columns)  # debug

    if df.empty:
        raise ValueError("CSV is empty")

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")

    return df


    if df.empty:
        raise ValueError("CSV is empty")

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")

    return df

def process(df, window):
    df["rolling_mean"] = df["close"].rolling(window=window).mean()
    df["signal"] = np.where(df["close"] > df["rolling_mean"], 1, 0)
    return df

def compute_metrics(df, start_time):
    rows_processed = len(df)
    signal_rate = df["signal"].mean()
    latency_ms = (time.time() - start_time) * 1000

    return {
        "rows_processed": int(rows_processed),
        "signal_rate": float(signal_rate),
        "latency_ms": float(latency_ms)
    }

def main():
    print("Script started")

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logger(args.log_file)
    logging.info("Starting batch job")
    start_time = time.time()

    try:
        config = load_config(args.config)
        print("Loaded config:", config) 
        np.random.seed(config["seed"])

        df = load_data(args.input)
        df = process(df, config["window"])

        metrics = compute_metrics(df, start_time)

        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=4)
        logging.info("Job completed successfully")
        logging.info(f"Metrics: {metrics}")   
    except Exception as e:
        print("ERROR:", e)

if __name__ == "__main__":
    main()