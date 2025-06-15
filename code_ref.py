import os
import re
import time
import pandas as pd
import requests
from openpyxl import load_workbook
from pdf2image import convert_from_path
from io import BytesIO

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

        if url.lower().endswith(".pdf"):
            with open("temp.pdf", "wb") as temp_pdf:
                temp_pdf.write(response.content)
            images = convert_from_path("temp.pdf")
            if images:
                images[0].save(filename, 'JPEG')
            else:
                raise ValueError("No images found in PDF")
        else:
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

        all_failed = True
        downloaded_status = []

        for url_col, local_path_col in cfg["url_columns"].items():
            url = row[url_col]
            tag = "front" if "Front" in url_col else "whole"
            filename = f"{nafdac_val}_{tag}.jpeg"
            file_path = os.path.join(cfg["output_dir"], filename)

            if pd.isna(url):
                continue

            if os.path.exists(file_path):
                df.at[idx, local_path_col] = f'=HYPERLINK("{file_path}", "{nafdac_val}_{tag}.jpeg")'
                all_failed = False
                downloaded_status.append(True)
                continue

            print(f"Downloading: {url}")
            _, success = download_image(url, file_path, cfg["timeout"])
            if success:
                df.at[idx, local_path_col] = f'=HYPERLINK("{file_path}", "{nafdac_val}_{tag}.jpeg")'
                success_counter["count"] += 1
                all_failed = False
                downloaded_status.append(True)
            else:
                df.at[idx, local_path_col] = "DOWNLOAD_FAILED"
                downloaded_status.append(False)

        if any(downloaded_status):
            full_image_path = os.path.join(cfg["output_dir"], f"{nafdac_val}.jpeg")
            df.at[idx, "Status"] = f'=HYPERLINK("{full_image_path}", "{nafdac_val}.jpeg")'
        else:
            df.at[idx, "Status"] = "Download Failed"

def save_with_local_paths(df, cfg):
    df.to_excel(cfg["output_excel"], index=False)

def extract_nafdac_from_filenames(folder_path):
    pattern = re.compile(r"(\d{8}-\d{4})")
    nafdac_list = []
    for filename in os.listdir(folder_path):
        match = pattern.search(filename)
        if match:
            nafdac_list.append(match.group(1))
    return nafdac_list

# === MAIN FUNCTION ===
def main(cfg):
    ensure_output_dir(cfg["output_dir"])

    df = pd.read_excel(cfg["excel_path"])
    df = clean_dataframe(df, cfg).reset_index(drop=True)

    required_cols = list(cfg["url_columns"].keys()) + [cfg["col1"]]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    total_rows = len(df)
    success_counter = {"count": 0}

    for start in range(0, total_rows, cfg["chunk_size"]):
        end = min(start + cfg["chunk_size"], total_rows)
        print(f"\n📦 Processing rows {start + 1} to {end}")
        process_chunk(df, cfg, start, end, success_counter)
        time.sleep(1)

    save_with_local_paths(df, cfg)

    extracted_nafdac = extract_nafdac_from_filenames(cfg["output_dir"])
    print("\n🔍 Extracted NAFDACNumbers from filenames:")
    print(extracted_nafdac)

    print(f"\n✅ Done. Updated Excel saved to: {cfg['output_excel']}")
    print(f"\n📥 Successfully downloaded {success_counter['count']} files.")

# === RUN ===
if __name__ == "__main__":
    main(CONFIG)
