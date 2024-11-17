import os
import pandas as pd
import pytz
from datetime import datetime, timedelta

# Define paths
base_dir = r"C:\Users\Eric\PycharmProjects\SchwabTradingApp"
schwab_dir = os.path.join(base_dir, "stock_data")
barchart_dir = os.path.join(schwab_dir, "barchart_data")
converted_dir = os.path.join(barchart_dir, "converted")

# Ensure the converted directory exists
os.makedirs(converted_dir, exist_ok=True)

# Process each file in the barchart_data folder
for filename in os.listdir(barchart_dir):
    filepath = os.path.join(barchart_dir, filename)

    # Skip directories and non-CSV files
    if not os.path.isfile(filepath) or not filename.endswith(".csv"):
        continue

    # Parse the symbol and timeframe from the filename
    try:
        # Split the filename by underscores and hyphens
        parts = filename.split("_")
        if len(parts) < 3:
            print(f"Skipping {filename}: Unable to extract symbol and timeframe.")
            continue

        # Extract symbol
        symbol = parts[0].upper()

        # Extract timeframe
        for part in parts:
            if "min" in part:
                timeframe = part.split("-")[-1]
                break
        else:
            print(f"Skipping {filename}: Timeframe not recognized.")
            continue

    except Exception as e:
        print(f"Error parsing {filename}: {e}")
        continue

    # Read Barchart data, skipping the footer line
    try:
        df = pd.read_csv(filepath, skipfooter=1, engine="python")
    except Exception as e:
        print(f"Failed to read {filename}: {e}")
        continue

    # Rename columns to match Schwab format
    try:
        df = df.rename(columns={
            "Time": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Last": "close",
            "Volume": "volume",
        })

        # Remove unnecessary columns
        df = df[["datetime", "open", "high", "low", "close", "volume"]]

        # Convert datetime column to datetime
        df["datetime"] = pd.to_datetime(df["datetime"], format="%m/%d/%Y %H:%M")

        # Adjust datetime to Central Time (subtracting 1 hour)
        df["datetime"] = df["datetime"] - timedelta(hours=1)

        # Format datetime to match Schwab format
        df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S") + "-05:00"

        # Reorder columns to match Schwab format
        df = df[["datetime", "open", "high", "low", "close", "volume"]]

        # Generate the Schwab-compatible filename using the date of the last row
        last_date = df["datetime"].iloc[-1][:10].replace("-", "")  # Get the last row's date
        schwab_filename = f"market_data_day_{symbol}_{timeframe}_{last_date}.csv"
        schwab_filepath = os.path.join(schwab_dir, schwab_filename)

        # Save the file in the Schwab format
        df.to_csv(schwab_filepath, index=False)
        print(f"Converted and saved: {schwab_filename}")

        # Move the original file to the converted directory
        os.rename(filepath, os.path.join(converted_dir, filename))
        print(f"Moved {filename} to {converted_dir}")

    except Exception as e:
        print(f"Failed to process {filename}: {e}")
