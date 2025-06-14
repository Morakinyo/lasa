import os
import requests
import pandas as pd
from urllib.parse import urlparse
from openpyxl import load_workbook

# === CONFIGURATION ===
excel_path = "image_urls.xlsx"        # Input Excel file path
output_dir = "downloaded_images"      # Folder to save images
url_column = "url"                    # Column name in Excel with URLs
local_path_column = "local_path"      # New column to store local paths
timeout = 10                          # Timeout for downloads

# === SETUP ===
os.makedirs(output_dir, exist_ok=True)

# Load Excel
try:
    df = pd.read_excel(excel_path)
except Exception as e:
    print(f"Error reading Excel file: {e}")
    exit()

if url_column not in df.columns:
    print(f"Column '{url_column}' not found in Excel file.")
    exit()

# Add a new column for local paths (initialize with empty strings)
df[local_path_column] = ""

# Helper: generate safe filenames
def get_filename(url, index):
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    if not name.lower().endswith(".jpeg"):
        name = f"image_{index}.jpeg"
    return f"{index:04d}_{name}"

# Download images and update DataFrame
for i, url in enumerate(df[url_column], start=1):
    if pd.isna(url):
        continue
    try:
        print(f"[{i}] Downloading: {url}")
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        filename = get_filename(url, i)
        file_path = os.path.join(output_dir, filename)
        with open(file_path, "wb") as f:
            f.write(response.content)
        df.at[i - 1, local_path_column] = file_path
        print(f"Saved to: {file_path}")
    except Exception as e:
        print(f"[{i}] Failed: {e}")
        df.at[i - 1, local_path_column] = "DOWNLOAD_FAILED"

# Save updated Excel
try:
    df.to_excel(excel_path, index=False)
    print(f"\nExcel file updated: {excel_path}")
except Exception as e:
    print(f"Failed to save Excel file: {e}")
