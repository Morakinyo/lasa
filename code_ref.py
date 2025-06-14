import os
import re
import time
import pandas as pd
import requests
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font
from openpyxl.worksheet.hyperlink import Hyperlink

# === CONFIGURATION ===
CONFIG = {
    "excel_path": "Medical Devices_Kunle.xlsx",
    "output_excel": "data_updated.xlsx",
    "output_dir": "downloaded_images",
    "url_columns": {
        "ProductFrontViewArtwork": "local_path_front",
        "ProductWholeViewArtwork": "local_path_whole"
    },
    "col1": "NAFDACNumber",
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
    if "ProductBrandName" in df.columns:
        df["ProductBrandName"] = df["ProductBrandName"].str.title()
    return df

def clean_filename(s):
    return re.sub(r"[^\w\-_. ]", "_", str(s)).replace(" ", "_")

def download_image(url, filename, timeout):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        with open(filename, "wb") as f:
            f.write(response.content)
        return filename, True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return "DOWNLOAD_FAILED", False

def process_chunk(df, cfg, start, end, success_counter):
    for idx in range(start, end):
        row = df.iloc[idx]
        nafdac_val = clean_filename(row[cfg["col1"]])

        for url_col, local_path_col in cfg["url_columns"].items():
            url = row[url_col]
            suffix = "front" if "Front" in url_col else "whole"
            filename = f"{nafdac_val}_{suffix}.jpeg"
            file_path = os.path.join(cfg["output_dir"], filename)

            if pd.isna(url):
                continue

            if os.path.exists(file_path):
                df.at[idx, local_path_col] = filename
                continue

            print(f"Downloading: {url}")
            result_path, success = download_image(url, file_path, cfg["timeout"])
            df.at[idx, local_path_col] = os.path.basename(result_path) if success else result_path
            if success:
                success_counter["count"] += 1

def save_with_hyperlinks(df, cfg):
    df_for_excel = df.copy()
    for col in cfg["url_columns"].values():
        df_for_excel[col] = df_for_excel[col].apply(
            lambda x: f'=HYPERLINK("{os.path.join(cfg["output_dir"], x)}", "{x}")' if x not in ["", "DOWNLOAD_FAILED"] else x
        )
    df_for_excel.to_excel(cfg["output_excel"], index=False)

# === MAIN FUNCTION ===
def main(cfg):
    ensure_output_dir(cfg["output_dir"])

    df = pd.read_excel(cfg["excel_path"])
    df = clean_dataframe(df, cfg).reset_index(drop=True)

    required_cols = list(cfg["url_columns"].keys()) + [cfg["col1"]]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    for local_path_col in cfg["url_columns"].values():
        if local_path_col not in df.columns:
            df[local_path_col] = ""

    total_rows = len(df)
    success_counter = {"count": 0}

    for start in range(0, total_rows, cfg["chunk_size"]):
        end = min(start + cfg["chunk_size"], total_rows)
        print(f"\n📦 Processing rows {start + 1} to {end}")
        process_chunk(df, cfg, start, end, success_counter)
        time.sleep(1)

    save_with_hyperlinks(df, cfg)

    print(f"\n✅ Done. Updated Excel saved to: {cfg['output_excel']}")
    print(f"\n📥 Successfully downloaded {success_counter['count']} files.")

# === RUN ===
if __name__ == "__main__":
    main(CONFIG)
