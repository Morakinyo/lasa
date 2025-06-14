import os
import re
import time
import pandas as pd
import requests

# === CONFIGURATION ===
CONFIG = {
    "excel_path": "Medical Devices_Kunle.xlsx",
    "output_excel": "data_updated.xlsx",
    "output_dir": "downloaded_images",
    "url_column": "ProductFrontViewArtwork",
    "col1": "NAFDACNumber",  # Only this column will be used in filename
    "local_path_column": "local_path",
    "timeout": 10,
    "chunk_size": 100,
    "date_pattern": r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2}\b",
    "tin_pattern": r"^\d{8}-\d{4}$",
    "required_non_null_columns": [
        "NAFDACNumber", "TIN", "ProductFrontViewArtwork", "ProductWholeViewArtwork"
    ],
    "date_like_column": "NAFDACNumber",
    "pattern_column": "TIN"
}

# === HELPER FUNCTIONS ===
def ensure_output_dir(path):
    os.makedirs(path, exist_ok=True)

def clean_dataframe(df, cfg):
    df = df[~df[cfg["date_like_column"]].astype(str).str.contains(cfg["date_pattern"], regex=True, na=False)]
    df = df[df[cfg["pattern_column"]].astype(str).str.match(cfg["tin_pattern"], na=False)]
    df = df.dropna(subset=cfg["required_non_null_columns"])
    return df

def clean_filename(s):
    return re.sub(r"[^\w\-_. ]", "_", str(s)).replace(" ", "_")

def download_image(url, filename, timeout):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        with open(filename, "wb") as f:
            f.write(response.content)
        return filename
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return "DOWNLOAD_FAILED"

def process_chunk(df, cfg, start, end):
    success_count = 0
    for idx in range(start, end):
        row = df.iloc[idx]
        url = row[cfg["url_column"]]

        if pd.isna(url):
            continue

        name_val = clean_filename(row[cfg["col1"]])
        filename = f"{name_val}.jpeg"
        file_path = os.path.join(cfg["output_dir"], filename)

        if os.path.exists(file_path):
            df.at[idx, cfg["local_path_column"]] = filename
            success_count += 1
            continue

        print(f"Downloading: {url}")
        result_path = download_image(url, file_path, cfg["timeout"])
        if result_path != "DOWNLOAD_FAILED":
            df.at[idx, cfg["local_path_column"]] = filename
            success_count += 1
        else:
            df.at[idx, cfg["local_path_column"]] = result_path

    return success_count

# === MAIN FUNCTION ===
def main(cfg):
    ensure_output_dir(cfg["output_dir"])

    df = pd.read_excel(cfg["excel_path"])
    df = clean_dataframe(df, cfg).reset_index(drop=True)


    for col in [cfg["url_column"], cfg["col1"]]:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    if cfg["local_path_column"] not in df.columns:
        df[cfg["local_path_column"]] = ""

    total_rows = len(df)
    for start in range(0, total_rows, cfg["chunk_size"]):
        end = min(start + cfg["chunk_size"], total_rows)
        print(f"\n📦 Processing rows {start + 1} to {end}")
        process_chunk(df, cfg, start, end)
        time.sleep(1)

    df.to_excel(cfg["output_excel"], index=False)
    print(f"\n✅ Done. Updated Excel saved to: {cfg['output_excel']}")

# === RUN ===
if __name__ == "__main__":
    main(CONFIG)
