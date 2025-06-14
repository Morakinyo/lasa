import os
import pandas as pd
import requests
from urllib.parse import urlparse

# === CONFIGURATION ===
excel_path = "data.xlsx"              # Input Excel file
output_dir = "downloaded_images"      # Folder to save images
url_column = "url"                    # Column name in Excel with image URLs
local_path_column = "local_path"      # New column for downloaded file paths
timeout = 10                          # Timeout in seconds for HTTP requests

# === SETUP ===
os.makedirs(output_dir, exist_ok=True)

# Load Excel
try:
    df = pd.read_excel(excel_path)
except Exception as e:
    print(f"Error reading Excel file: {e}")
    exit()

if url_column not in df.columns:
    print(f"URL column '{url_column}' not found in Excel file.")
    exit()

# Create empty column for file paths
df[local_path_column] = ""

# Helper function to generate filenames
def generate_filename(url, index):
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    ext = os.path.splitext(name)[1]
    if not ext.lower().endswith(".jpeg"):
        ext = ".jpeg"
    return f"{index:04d}_{os.path.splitext(name)[0]}{ext}"

# Loop through URLs
for idx, row in df.iterrows():
    url = row[url_column]
    if pd.isna(url):
        continue

    try:
        print(f"Downloading ({idx + 1}): {url}")
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        filename = generate_filename(url, idx + 1)
        file_path = os.path.join(output_dir, filename)

        with open(file_path, "wb") as f:
            f.write(response.content)

        df.at[idx, local_path_column] = file_path
        print(f"Saved: {file_path}")

    except Exception as e:
        print(f"Failed to download URL at row {idx + 1}: {e}")
        df.at[idx, local_path_column] = "DOWNLOAD_FAILED"

# Save updated Excel
output_excel_path = "data_updated.xlsx"
try:
    df.to_excel(output_excel_path, index=False)
    print(f"\n✅ Excel file updated: {output_excel_path}")
except Exception as e:
    print(f"Failed to save updated Excel: {e}")
