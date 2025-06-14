import os
import pandas as pd
import requests
from urllib.parse import urlparse
import re
import time

# === CONFIGURATION ===
excel_path = "data.xlsx"                  # Path to input Excel file
output_excel = "data_updated.xlsx"        # Output Excel with updated paths
output_dir = "downloaded_images"          # Folder to save images
url_column = "url"                        # Column containing image URLs
col1 = "name"                             # First column to use in filename
col2 = "category"                         # Second column to use in filename
local_path_column = "local_path"          # New column to store local file paths
timeout = 10                              # Timeout in seconds for HTTP request
chunk_size = 100                          # Number of rows to process at a time

# === HELPER FUNCTIONS ===

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Clean strings to be safe filenames


def clean_multframe(df, 
                    date_like_column=None,
                    date_like_pattern=None,
                    pattern_column=None,
                    required_pattern=None,
                    non_null_columns=None):
    """
    Cleans a DataFrame by:
    1. Removing rows with date-like strings in a specific column.
    2. Removing rows where another column does not match a required pattern.
    3. Removing rows with null values in specified columns.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        date_like_column (str): Name of column to check for date-like strings.
        date_like_pattern (str): Regex pattern to identify date-like strings
        pattern_column (str): Name of column to check for required pattern.
        required_pattern (str): Regex pattern the values in `pattern_column` must match.
        non_null_columns (list): List of column names that must not be null.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    if date_like_column and date_like_pattern:
        df = df[~df[date_like_column].astype(str).str.contains(date_like_pattern, regex=True, na=False)]

    if pattern_column and required_pattern:
        df = df[df[pattern_column].astype(str).str.match(required_pattern, na=False)]

    if non_null_columns:
        df = df.dropna(subset=non_null_columns)

    return df

date_pattern = r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2}\b'
tin_pattern = r'^\d{8}-\d{4}$'



df = pd.read_excel(excel_path)
cleaned_df = clean_multframe(
    df,
    date_like_column='NAFDACNumber',
    date_like_pattern=date_pattern,
    pattern_column='TIN',
    required_pattern=tin_pattern,
    non_null_columns=['NAFDACNumber', 'TIN', 'ProductFrontViewArtwork','ProductWholeViewArtwork']
)

cleaned_df

def clean_filename(s):
    s = str(s)
    s = re.sub(r"[^\w\-_. ]", "_", s)
    return s.replace(" ", "_")

# Download a single image
def download_image(url, filename):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        with open(filename, "wb") as f:
            f.write(response.content)
        return filename
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return "DOWNLOAD_FAILED"

# === MAIN PROCESS ===

# Load Excel file
df = pd.read_excel(excel_path)

# Ensure required columns exist
for col in [url_column, col1, col2]:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# Add column to store local paths
if local_path_column not in df.columns:
    df[local_path_column] = ""

# Process in chunks
total_rows = len(df)
for start in range(0, total_rows, chunk_size):
    end = min(start + chunk_size, total_rows)
    print(f"\n📦 Processing rows {start + 1} to {end}")

    for idx in range(start, end):
        row = df.iloc[idx]
        url = row[url_column]
        name_val = clean_filename(row[col1])
        cat_val = clean_filename(row[col2])

        if pd.isna(url):
            continue

        filename = f"{name_val}_{cat_val}.jpeg"
        file_path = os.path.join(output_dir, filename)

        # Skip if already downloaded
        if os.path.exists(file_path):
            df.at[idx, local_path_column] = file_path
            continue

        print(f"Downloading: {url}")
        result_path = download_image(url, file_path)
        df.at[idx, local_path_column] = result_path

    # Optional delay between chunks to avoid server throttling
    time.sleep(1)

# Save updated Excel file
df.to_excel(output_excel, index=False)
print(f"\n✅ Done. Updated Excel saved to: {output_excel}")
